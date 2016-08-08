/*******************************************************************************
 * Copyright (c) 1999, 2014 IBM Corp.
 * <p/>
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 * <p/>
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package org.eclipse.paho.android.service;

import android.app.Notification;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Bundle;
import android.os.PowerManager;
import android.os.PowerManager.WakeLock;
import android.support.v4.app.NotificationCompat;
import android.util.Log;

import org.eclipse.paho.android.service.MessageStore.StoredMessage;
import org.eclipse.paho.android.service.sample.R;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * <p>
 * MqttConnection holds a MqttAsyncClient {host,port,clientId} instance to perform 
 * MQTT operations to MQTT broker.
 * </p>
 * <p>
 * Most of the major API here is intended to implement the most general forms of
 * the methods in IMqttAsyncClient, with slight adjustments for the Android
 * environment<br>
 * These adjustments usually consist of adding two parameters to each method :-
 * <ul>
 * <li>invocationContext - a string passed from the application to identify the
 * context of the operation (mainly included for support of the javascript API
 * implementation)</li>
 * <li>activityToken - a string passed from the Activity to relate back to a
 * callback method or other context-specific data</li>
 * </ul>
 * </p>
 * <p>
 * Operations are very much asynchronous, so success and failure are notified by
 * packing the relevant data into Intent objects which are broadcast back to the
 * Activity via the MqttService.callbackToActivity() method.
 * </p>
 */
class MqttConnection implements MqttCallback {

    // Strings for Intents etc..
    private static final String TAG = "MqttConnection";
    // Error status messages
    private static final String NOT_CONNECTED = "not connected";

    // fields for the connection definition
    private String serverURI;

    public String getServerURI() {
        return serverURI;
    }

    public void setServerURI(String serverURI) {
        this.serverURI = serverURI;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    private String clientId;
    private MqttClientPersistence persistence = null;
    private MqttConnectOptions connectOptions;

    public MqttConnectOptions getConnectOptions() {
        return connectOptions;
    }

    public void setConnectOptions(MqttConnectOptions connectOptions) {
        this.connectOptions = connectOptions;
    }

    // Client handle, used for callbacks...
    private String clientHandle;

    public String getClientHandle() {
        return clientHandle;
    }

    public void setClientHandle(String clientHandle) {
        this.clientHandle = clientHandle;
    }

    //store connect ActivityToken for reconnect
    private String reconnectActivityToken = null;

    // our client object - instantiated on connect
    private MqttAsyncClient myClient = null;

    // our (parent) service object
    private MqttService service = null;

    private volatile boolean disconnected = true;
    private boolean cleanSession = true;

    // Indicate this connection is connecting or not.
    // This variable uses to avoid reconnect multiple times.
    private volatile boolean isConnecting = false;

    // Saved sent messages and their corresponding Topics, activityTokens and
    // invocationContexts, so we can handle "deliveryComplete" callbacks
    // from the mqttClient
    private Map<IMqttDeliveryToken, String /* Topic */> savedTopics = new HashMap<IMqttDeliveryToken, String>();
    private Map<IMqttDeliveryToken, MqttMessage> savedSentMessages = new HashMap<IMqttDeliveryToken, MqttMessage>();
    private Map<IMqttDeliveryToken, String> savedActivityTokens = new HashMap<IMqttDeliveryToken, String>();
    private Map<IMqttDeliveryToken, String> savedInvocationContexts = new HashMap<IMqttDeliveryToken, String>();

    private WakeLock wakelock = null;
    private String wakeLockTag = null;

    /**
     * Constructor - create an MqttConnection to communicate with MQTT server
     *
     * @param service
     *            our "parent" service - we make callbacks to it
     * @param serverURI
     *            the URI of the MQTT server to which we will connect
     * @param clientId
     *            the name by which we will identify ourselves to the MQTT
     *            server
     * @param persistence
     *            the persistence class to use to store in-flight message. If
     *            null then the default persistence mechanism is used
     * @param clientHandle
     *            the "handle" by which the activity will identify us
     */
    MqttConnection(MqttService service, String serverURI, String clientId,
                   MqttClientPersistence persistence, String clientHandle) {
        Log.v("mqtt", "MqttConnection");
        this.serverURI = serverURI.toString();
        this.service = service;
        this.clientId = clientId;
        this.persistence = persistence;
        this.clientHandle = clientHandle;

        StringBuffer buff = new StringBuffer(this.getClass().getCanonicalName());
        buff.append(" ");
        buff.append(clientId);
        buff.append(" ");
        buff.append("on host ");
        buff.append(serverURI);
        wakeLockTag = buff.toString();
    }

    // The major API implementation follows

    /**
     * Connect to the server specified when we were instantiated
     *
     * @param options
     *            timeout, etc
     * @param invocationContext
     *            arbitrary data to be passed back to the application
     * @param activityToken
     *            arbitrary identifier to be passed back to the Activity
     */
    public void connect(MqttConnectOptions options, String invocationContext,
                        String activityToken) {
        Log.v("mqtt", "MqttConnection connect trace 1");
        connectOptions = options;
        reconnectActivityToken = activityToken;

        if (options != null) {
            cleanSession = options.isCleanSession();
            Log.v("mqtt", "MqttConnection connect trace 2");
        }

        if (connectOptions.isCleanSession()) { // if it's a clean session,
            // discard old data
            service.messageStore.clearArrivedMessages(clientHandle);
            Log.v("mqtt", "MqttConnection connect trace 3");
        }
        Log.v("mqtt", "MqttConnection connect trace 4");
        service.traceDebug(TAG, "Connecting {" + serverURI + "} as {" + clientId + "}");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                activityToken);
        resultBundle.putString(
                MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT,
                invocationContext);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.CONNECT_ACTION);


        try {
            if (persistence == null) {
                Log.v("mqtt", "MqttConnection connect trace 5");
                // ask Android where we can put files
                File myDir = service.getExternalFilesDir(TAG);
               // Log.v("mqtt", "MqttConnection connect trace 5a "+myDir.getAbsolutePath());

                /*File myDir = new File(Environment.getExternalStorageDirectory() + "/MqttConnection");
                Log.v("mqtt","MqttConnection myDir.getAbsolutePath(): "+myDir.getAbsolutePath());
                boolean success = true;
                if (!myDir.exists()) {
                    success = myDir.mkdir();
                }
                if (success) {
                    // Do something on success
                } else {
                    // Do something else on failure
                }*/

                if (myDir == null) {
                    // No external storage, use internal storage instead.
                    myDir = service.getDir(TAG, Context.MODE_PRIVATE);
                    Log.v("mqtt", "MqttConnection connect trace 6");
                    if (myDir == null) {
                        //Shouldn't happen.
                        Log.v("mqtt", "MqttConnection connect trace 7");
                        resultBundle.putString(
                                MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                                "Error! No external and internal storage available");
                        resultBundle.putSerializable(
                                MqttServiceConstants.CALLBACK_EXCEPTION, new MqttPersistenceException());
                        service.callbackToActivity(clientHandle, Status.ERROR,
                                resultBundle);
                        return;
                    }
                }
                Log.v("mqtt", "MqttConnection connect trace 8");
                // use that to setup MQTT client persistence storage
                persistence = new MqttDefaultFilePersistence(
                        myDir.getAbsolutePath());
            }

            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle) {

                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    Log.v("mqtt", "MqttConnection connect trace 9 onSuccess");
                    doAfterConnectSuccess(resultBundle);
                    service.traceDebug(TAG, "connect success!");
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken,
                                      Throwable exception) {
                    Log.v("mqtt", "MqttConnection connect trace 9 onFailure");
                    resultBundle.putString(
                            MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                            exception.getLocalizedMessage());
                    resultBundle.putSerializable(
                            MqttServiceConstants.CALLBACK_EXCEPTION, exception);
                    service.traceError(TAG,
                            "connect fail, call connect to reconnect.reason:"
                                    + exception.getMessage());

                    doAfterConnectFail(resultBundle);

                }
            };
            Log.v("mqtt", "MqttConnection connect trace 10");
            if (myClient != null) {
                Log.v("mqtt", "MqttConnection connect trace 11");
                if (isConnecting) {
                    Log.v("mqtt", "MqttConnection connect trace 12");
                    service.traceDebug(TAG,
                            "myClient != null and the client is connecting. Connect return directly.");
                    service.traceDebug(TAG, "Connect return:isConnecting:" + isConnecting + ".disconnected:" + disconnected);
                    return;
                } else if (!disconnected) {
                    Log.v("mqtt", "MqttConnection connect trace 13");
                    service.traceDebug(TAG, "myClient != null and the client is connected and notify!");
                    doAfterConnectSuccess(resultBundle);
                } else {
                    Log.v("mqtt", "MqttConnection connect trace 14");
                    service.traceDebug(TAG, "myClient != null and the client is not connected");
                    service.traceDebug(TAG, "Do Real connect!");
                    setConnectingState(true);
                    myClient.connect(connectOptions, invocationContext, listener);
                }
            }

            // if myClient is null, then create a new connection
            else {
                Log.v("mqtt", "MqttConnection connect trace 15");
                myClient = new MqttAsyncClient(serverURI, clientId,
                        persistence, new AlarmPingSender(service));
                myClient.setCallback(this);

                service.traceDebug(TAG, "Do Real connect!");
                setConnectingState(true);
                myClient.connect(connectOptions, invocationContext, listener);
            }
        } catch (Exception e) {
            handleException(resultBundle, e);
        }
    }

    private void doAfterConnectSuccess(final Bundle resultBundle) {
        Log.v("mqtt", "MqttConnection doAfterConnectSuccess");
        //since the device's cpu can go to sleep, acquire a wakelock and drop it later.
        acquireWakeLock();
        service.callbackToActivity(clientHandle, Status.OK, resultBundle);
        deliverBacklog();
        setConnectingState(false);
        disconnected = false;
        releaseWakeLock();
    }

    private void doAfterConnectFail(final Bundle resultBundle) {
        //
        Log.v("mqtt", "MqttConnection doAfterConnectFail");
        acquireWakeLock();
        disconnected = true;
        setConnectingState(false);
        service.callbackToActivity(clientHandle, Status.ERROR, resultBundle);
        releaseWakeLock();
    }

    private void handleException(final Bundle resultBundle, Exception e) {
        resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                e.getLocalizedMessage());

        resultBundle.putSerializable(MqttServiceConstants.CALLBACK_EXCEPTION, e);

        service.callbackToActivity(clientHandle, Status.ERROR, resultBundle);
    }

    /**
     * Attempt to deliver any outstanding messages we've received but which the
     * application hasn't acknowledged. If "cleanSession" was specified, we'll
     * have already purged any such messages from our messageStore.
     */
    private void deliverBacklog() {
        Iterator<StoredMessage> backlog = service.messageStore
                .getAllArrivedMessages(clientHandle);
        while (backlog.hasNext()) {
            StoredMessage msgArrived = backlog.next();
            Bundle resultBundle = messageToBundle(msgArrived.getMessageId(),
                    msgArrived.getTopic(), msgArrived.getMessage());
            resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                    MqttServiceConstants.MESSAGE_ARRIVED_ACTION);
            service.callbackToActivity(clientHandle, Status.OK, resultBundle);
        }
    }

    /**
     * Create a bundle containing all relevant data pertaining to a message
     *
     * @param messageId
     *            the message's identifier in the messageStore, so that a
     *            callback can be made to remove it once delivered
     * @param topic
     *            the topic on which the message was delivered
     * @param message
     *            the message itself
     * @return the bundle
     */
    private Bundle messageToBundle(String messageId, String topic,
                                   MqttMessage message) {
        Bundle result = new Bundle();
        result.putString(MqttServiceConstants.CALLBACK_MESSAGE_ID, messageId);
        result.putString(MqttServiceConstants.CALLBACK_DESTINATION_NAME, topic);
        result.putParcelable(MqttServiceConstants.CALLBACK_MESSAGE_PARCEL,
                new ParcelableMqttMessage(message));
        return result;
    }

    /**
     * Close connection from the server
     *
     */
    void close() {
        Log.v("mqtt", "MqttConnection close()");
        service.traceDebug(TAG, "close()");
        try {
            if (myClient != null) {
                myClient.close();
            }
        } catch (MqttException e) {
            // Pass a new bundle, let handleException stores error messages.
            handleException(new Bundle(), e);
        }
    }

    /**
     * Disconnect from the server
     *
     * @param quiesceTimeout
     *            in milliseconds
     * @param invocationContext
     *            arbitrary data to be passed back to the application
     * @param activityToken
     *            arbitrary string to be passed back to the activity
     */
    void disconnect(long quiesceTimeout, String invocationContext,
                    String activityToken) {
        Log.v("mqtt", "MqttConnection disconnect()");
        service.traceDebug(TAG, "disconnect()");
        disconnected = true;
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                activityToken);
        resultBundle.putString(
                MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT,
                invocationContext);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.DISCONNECT_ACTION);
        if ((myClient != null) && (myClient.isConnected())) {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try {
                myClient.disconnect(quiesceTimeout, invocationContext, listener);
            } catch (Exception e) {
                handleException(resultBundle, e);
            }
        } else {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                    NOT_CONNECTED);
            service.traceError(MqttServiceConstants.DISCONNECT_ACTION,
                    NOT_CONNECTED);
            service.callbackToActivity(clientHandle, Status.ERROR, resultBundle);
        }

        if (connectOptions.isCleanSession()) {
            // assume we'll clear the stored messages at this point
            service.messageStore.clearArrivedMessages(clientHandle);
        }

        releaseWakeLock();
    }

    /**
     * Disconnect from the server
     *
     * @param invocationContext
     *            arbitrary data to be passed back to the application
     * @param activityToken
     *            arbitrary string to be passed back to the activity
     */
    void disconnect(String invocationContext, String activityToken) {
        Log.v("mqtt", "MqttConnection disconnect() 2");
        service.traceDebug(TAG, "disconnect()");
        disconnected = true;
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                activityToken);
        resultBundle.putString(
                MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT,
                invocationContext);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.DISCONNECT_ACTION);
        if ((myClient != null) && (myClient.isConnected())) {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try {
                myClient.disconnect(invocationContext, listener);
            } catch (Exception e) {
                handleException(resultBundle, e);
            }
        } else {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                    NOT_CONNECTED);
            service.traceError(MqttServiceConstants.DISCONNECT_ACTION,
                    NOT_CONNECTED);
            service.callbackToActivity(clientHandle, Status.ERROR, resultBundle);
        }

        if (connectOptions.isCleanSession()) {
            // assume we'll clear the stored messages at this point
            service.messageStore.clearArrivedMessages(clientHandle);
        }
        releaseWakeLock();
    }

    /**
     * @return true if we are connected to an MQTT server
     */
    public boolean isConnected() {
        Log.v("mqtt", "MqttConnection isConnected()");
        if (myClient != null)
            return myClient.isConnected();
        return false;
    }

    /**
     * Publish a message on a topic
     *
     * @param topic
     *            the topic on which to publish - represented as a string, not
     *            an MqttTopic object
     * @param payload
     *            the content of the message to publish
     * @param qos
     *            the quality of service requested
     * @param retained
     *            whether the MQTT server should retain this message
     * @param invocationContext
     *            arbitrary data to be passed back to the application
     * @param activityToken
     *            arbitrary string to be passed back to the activity
     * @return token for tracking the operation
     */
    public IMqttDeliveryToken publish(String topic, byte[] payload, int qos,
                                      boolean retained, String invocationContext, String activityToken) {
        Log.v("mqtt", "MqttConnection publish");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.SEND_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                activityToken);
        resultBundle.putString(
                MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT,
                invocationContext);

        IMqttDeliveryToken sendToken = null;

        if ((myClient != null) && (myClient.isConnected())) {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try {
                MqttMessage message = new MqttMessage(payload);
                message.setQos(qos);
                message.setRetained(retained);
                sendToken = myClient.publish(topic, payload, qos, retained,
                        invocationContext, listener);
                storeSendDetails(topic, message, sendToken, invocationContext,
                        activityToken);
            } catch (Exception e) {
                handleException(resultBundle, e);
            }
        } else {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                    NOT_CONNECTED);
            service.traceError(MqttServiceConstants.SEND_ACTION, NOT_CONNECTED);
            service.callbackToActivity(clientHandle, Status.ERROR, resultBundle);
        }

        return sendToken;
    }

    /**
     * Publish a message on a topic
     *
     * @param topic
     *            the topic on which to publish - represented as a string, not
     *            an MqttTopic object
     * @param message
     *            the message to publish
     * @param invocationContext
     *            arbitrary data to be passed back to the application
     * @param activityToken
     *            arbitrary string to be passed back to the activity
     * @return token for tracking the operation
     */
    public IMqttDeliveryToken publish(String topic, MqttMessage message,
                                      String invocationContext, String activityToken) {
        Log.v("mqtt", "MqttConnection publish 2");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.SEND_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                activityToken);
        resultBundle.putString(
                MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT,
                invocationContext);

        IMqttDeliveryToken sendToken = null;

        if ((myClient != null) && (myClient.isConnected())) {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try {
                sendToken = myClient.publish(topic, message, invocationContext,
                        listener);
                storeSendDetails(topic, message, sendToken, invocationContext,
                        activityToken);
            } catch (Exception e) {
                handleException(resultBundle, e);
            }
        } else {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                    NOT_CONNECTED);
            service.traceError(MqttServiceConstants.SEND_ACTION, NOT_CONNECTED);
            service.callbackToActivity(clientHandle, Status.ERROR, resultBundle);
        }
        return sendToken;
    }

    /**
     * Subscribe to a topic
     *
     * @param topic
     *            a possibly wildcarded topic name
     * @param qos
     *            requested quality of service for the topic
     * @param invocationContext
     *            arbitrary data to be passed back to the application
     * @param activityToken
     *            arbitrary identifier to be passed back to the Activity
     */
    public void subscribe(final String topic, final int qos,
                          String invocationContext, String activityToken) {
        Log.v("mqtt", "MqttConnection subscribe");
        service.traceDebug(TAG, "subscribe({" + topic + "}," + qos + ",{"
                + invocationContext + "}, {" + activityToken + "}");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.SUBSCRIBE_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                activityToken);
        resultBundle.putString(
                MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT,
                invocationContext);

        if ((myClient != null) && (myClient.isConnected())) {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try {
                myClient.subscribe(topic, qos, invocationContext, listener);
            } catch (Exception e) {
                handleException(resultBundle, e);
            }
        } else {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                    NOT_CONNECTED);
            service.traceError("subscribe", NOT_CONNECTED);
            service.callbackToActivity(clientHandle, Status.ERROR, resultBundle);
        }
    }

    /**
     * Subscribe to one or more topics
     *
     * @param topic
     *            a list of possibly wildcarded topic names
     * @param qos
     *            requested quality of service for each topic
     * @param invocationContext
     *            arbitrary data to be passed back to the application
     * @param activityToken
     *            arbitrary identifier to be passed back to the Activity
     */
    public void subscribe(final String[] topic, final int[] qos,
                          String invocationContext, String activityToken) {
        Log.v("mqtt", "MqttConnection subscribe 2");
        service.traceDebug(TAG, "subscribe({" + topic + "}," + qos + ",{"
                + invocationContext + "}, {" + activityToken + "}");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.SUBSCRIBE_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                activityToken);
        resultBundle.putString(
                MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT,
                invocationContext);

        if ((myClient != null) && (myClient.isConnected())) {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try {
                myClient.subscribe(topic, qos, invocationContext, listener);
            } catch (Exception e) {
                handleException(resultBundle, e);
            }
        } else {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                    NOT_CONNECTED);
            service.traceError("subscribe", NOT_CONNECTED);
            service.callbackToActivity(clientHandle, Status.ERROR, resultBundle);
        }
    }

    /**
     * Unsubscribe from a topic
     *
     * @param topic
     *            a possibly wildcarded topic name
     * @param invocationContext
     *            arbitrary data to be passed back to the application
     * @param activityToken
     *            arbitrary identifier to be passed back to the Activity
     */
    void unsubscribe(final String topic, String invocationContext,
                     String activityToken) {
        Log.v("mqtt", "MqttConnection unsubscribe");
        service.traceDebug(TAG, "unsubscribe({" + topic + "},{"
                + invocationContext + "}, {" + activityToken + "})");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.UNSUBSCRIBE_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                activityToken);
        resultBundle.putString(
                MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT,
                invocationContext);
        if ((myClient != null) && (myClient.isConnected())) {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try {
                myClient.unsubscribe(topic, invocationContext, listener);
            } catch (Exception e) {
                handleException(resultBundle, e);
            }
        } else {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                    NOT_CONNECTED);

            service.traceError("subscribe", NOT_CONNECTED);
            service.callbackToActivity(clientHandle, Status.ERROR, resultBundle);
        }
    }

    /**
     * Unsubscribe from one or more topics
     *
     * @param topic
     *            a list of possibly wildcarded topic names
     * @param invocationContext
     *            arbitrary data to be passed back to the application
     * @param activityToken
     *            arbitrary identifier to be passed back to the Activity
     */
    void unsubscribe(final String[] topic, String invocationContext,
                     String activityToken) {
        Log.v("mqtt", "MqttConnection unsubscribe 2");
        service.traceDebug(TAG, "unsubscribe({" + topic + "},{"
                + invocationContext + "}, {" + activityToken + "})");
        final Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.UNSUBSCRIBE_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                activityToken);
        resultBundle.putString(
                MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT,
                invocationContext);
        if ((myClient != null) && (myClient.isConnected())) {
            IMqttActionListener listener = new MqttConnectionListener(
                    resultBundle);
            try {
                myClient.unsubscribe(topic, invocationContext, listener);
            } catch (Exception e) {
                handleException(resultBundle, e);
            }
        } else {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                    NOT_CONNECTED);

            service.traceError("subscribe", NOT_CONNECTED);
            service.callbackToActivity(clientHandle, Status.ERROR, resultBundle);
        }
    }

    /**
     * Get tokens for all outstanding deliveries for a client
     *
     * @return an array (possibly empty) of tokens
     */
    public IMqttDeliveryToken[] getPendingDeliveryTokens() {
        Log.v("mqtt", "MqttConnection getPendingDeliveryTokens()");
        return myClient.getPendingDeliveryTokens();
    }

    // Implement MqttCallback

    /**
     * Callback for connectionLost
     *
     * @param why
     *            the exeception causing the break in communications
     */
    @Override
    public void connectionLost(Throwable why) {
        Log.v("mqtt", "MqttConnection connectionLost()");
        service.traceDebug(TAG, "connectionLost(" + why.getMessage() + ")");
        disconnected = true;
        try {
            myClient.disconnect(null, new IMqttActionListener() {

                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    // No action
                    Log.v("mqtt", "MqttConnection connectionLost() disconnect onSuccess");
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken,
                                      Throwable exception) {
                    // No action
                    Log.v("mqtt", "MqttConnection connectionLost() disconnect onFailure");
                }
            });
        } catch (Exception e) {
            // ignore it - we've done our best
        }

        Bundle resultBundle = new Bundle();
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.ON_CONNECTION_LOST_ACTION);
        if (why != null) {
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                    why.getMessage());
            if (why instanceof MqttException) {
                resultBundle.putSerializable(
                        MqttServiceConstants.CALLBACK_EXCEPTION, why);
            }
            resultBundle.putString(
                    MqttServiceConstants.CALLBACK_EXCEPTION_STACK,
                    Log.getStackTraceString(why));
        }
        service.callbackToActivity(clientHandle, Status.OK, resultBundle);
        // client has lost connection no need for wake lock
        releaseWakeLock();
    }

    /**
     * Callback to indicate a message has been delivered (the exact meaning of
     * "has been delivered" is dependent on the QOS value)
     *
     * @param messageToken
     *            the messge token provided when the message was originally sent
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken messageToken) {
        Log.v("mqtt", "MqttConnection deliveryComplete()");
        service.traceDebug(TAG, "deliveryComplete(" + messageToken + ")");

        MqttMessage message = savedSentMessages.remove(messageToken);
        if (message != null) { // If I don't know about the message, it's
            // irrelevant
            String topic = savedTopics.remove(messageToken);
            String activityToken = savedActivityTokens.remove(messageToken);
            String invocationContext = savedInvocationContexts
                    .remove(messageToken);

            Bundle resultBundle = messageToBundle(null, topic, message);
            if (activityToken != null) {
                resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                        MqttServiceConstants.SEND_ACTION);
                resultBundle.putString(
                        MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                        activityToken);
                resultBundle.putString(
                        MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT,
                        invocationContext);

                service.callbackToActivity(clientHandle, Status.OK,
                        resultBundle);
            }
            resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                    MqttServiceConstants.MESSAGE_DELIVERED_ACTION);
            service.callbackToActivity(clientHandle, Status.OK, resultBundle);
        }

        // this notification will have kept the connection alive but send the previously sechudled ping anyway
    }

    /**
     * Callback when a message is received
     *
     * @param topic
     *            the topic on which the message was received
     * @param message
     *            the message itself
     */
    @Override
    public void messageArrived(String topic, MqttMessage message)
            throws Exception {
        Log.v("mqtt", "MqttConnection messageArrived()");
        service.traceDebug(TAG,
                "messageArrived(" + topic + ",{" + message.toString() + "})");

        String messageId = service.messageStore.storeArrived(clientHandle,
                topic, message);

        parseMqttMessageV2(topic, message);

        Bundle resultBundle = messageToBundle(messageId, topic, message);
        resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                MqttServiceConstants.MESSAGE_ARRIVED_ACTION);
        resultBundle.putString(MqttServiceConstants.CALLBACK_MESSAGE_ID,
                messageId);
        service.callbackToActivity(clientHandle, Status.OK, resultBundle);

    }

    private void parseMqttMessageV2(String topic, MqttMessage mqttMessage) throws Exception {
        Log.v("mqtt", "parseMqttMessageV2");
        Context ctx = NanuService.getContext();
        byte origMqttMsgByte[] = mqttMessage.getPayload();
        int mqttIndex = 0;
        boolean processVTagSuccess = false;

        boolean processPTagSuccess = false;
        long mqttPacketValue = 0;

        boolean processTTagSuccess = false;
        long mqttTimestampValue = 0;

        boolean processLTagSuccess = false;
        int mqttMsgLengthValue = 0;

        boolean processMTagSuccess = false;
        String mqttMessageValue = "";
        String mqttMembersValue = "";

        boolean processGTagSuccess = false;
        long mqttGroupIDValue = 0;

        boolean processSTagSuccess = false;
        String mqttSubjectValue = "";

        boolean processCTagSuccess = false;
        int mqttMemberCountValue = 0;

        boolean processNTagSuccess = false;
        int mqttAdminCountValue = 0;

        boolean processATagSuccess = false;
        String mqttAdminsValue = "";

        String[] topicArray = topic.split("\\/");

        String sender = topicArray[2];
        if (topicArray.length == 4) {
            processGTagSuccess = true;
            try {
                mqttGroupIDValue = Long.parseLong(topicArray[3].toString().trim());
            } catch (NumberFormatException nfe) {
                processGTagSuccess = false;
                nfe.printStackTrace();
            }

            if (mqttGroupIDValue == 0) {
                try {
                    mqttGroupIDValue = Long.valueOf(topicArray[3].trim());
                } catch (Exception err) {
                    processGTagSuccess = false;
                    err.printStackTrace();
                }
            }
        }

        String mqttMsgDateValue = "";
        for (int indexMqttCounter = 0; indexMqttCounter < origMqttMsgByte.length; indexMqttCounter++) {
                /*       Log.v(SettingsManager.TAG, "MqttService origMqttMsgByte[" + indexMqttCounter + "] = " + origMqttMsgByte[indexMqttCounter]); */
        }

        for (int indexMqttCounter = 0; indexMqttCounter < origMqttMsgByte.length; indexMqttCounter++) {
            if (indexMqttCounter == 0) {
                mqttIndex = indexMqttCounter;
                long mqttVTag = getMqttTag(origMqttMsgByte, mqttIndex);
                if (mqttVTag != -1) {
                    if (mqttVTag == 86) // "V"
                    {
                        processVTagSuccess = true;
                        mqttIndex = mqttIndex + 2;
                    } else {
                        processVTagSuccess = false;
                        break;
                    }
                }
            } else {
                if (mqttIndex == indexMqttCounter) {
                    long mqttTag = getMqttTag(origMqttMsgByte, mqttIndex);
                    if (mqttTag != -1) {
                        if (mqttTag == 80) /* "P" */ {
                            mqttIndex = mqttIndex + 1;
                            long mPValue = origMqttMsgByte[mqttIndex];
                            mqttPacketValue = mPValue;
                            mqttIndex = mqttIndex + 1;
                            processPTagSuccess = true;
                        } else if (mqttTag == 84) /* "T" */ {
                            mqttIndex = mqttIndex + 1;
                            byte timeStampArray[] = new byte[8];
                            for (int i = 0; i < 8; i++) {
                                timeStampArray[i] = origMqttMsgByte[mqttIndex + i];
                            }
                            mqttTimestampValue = ByteBuffer.wrap(timeStampArray).order(ByteOrder.LITTLE_ENDIAN).getLong();

                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
                            String messageYear = sdf.format(mqttTimestampValue);
                            if (messageYear.length() != 4) {
                                mqttTimestampValue = ByteBuffer.wrap(timeStampArray).order(ByteOrder.BIG_ENDIAN).getLong();
                            }
                            SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            String messageDate = sdfDate.format(mqttTimestampValue);
                            processTTagSuccess = true;
                            mqttIndex = mqttIndex + 8;
                        } else if (mqttTag == 76) /* "L" */ {
                            if (processPTagSuccess) {
                                if (mqttPacketValue == -128 || (mqttPacketValue == -117) || (mqttPacketValue == -115) || (mqttPacketValue == -126)) {
                                    mqttIndex = mqttIndex + 1;
                                    mqttMsgLengthValue = origMqttMsgByte[mqttIndex];
                                    processLTagSuccess = true;
                                    mqttIndex = mqttIndex + 1;
                                } else if (mqttPacketValue == 0) {
                                    mqttIndex = mqttIndex + 1;
                                    byte msgLengthArray[] = new byte[4];
                                    for (int i = 0; i < 4; i++) {
                                        msgLengthArray[i] = origMqttMsgByte[mqttIndex + i];
                                    }
                                    mqttMsgLengthValue = ByteBuffer.wrap(msgLengthArray).order(ByteOrder.LITTLE_ENDIAN).getInt();
                                    processLTagSuccess = true;
                                    mqttIndex = mqttIndex + 4;
                                }
                            }
                        } else if (mqttTag == 77) /* "M" */ {
                            if (processPTagSuccess) {
                                if ((mqttPacketValue == -128) || (mqttPacketValue == -124) || (mqttPacketValue == -126) || (mqttPacketValue == -117)) {

                                    if (processCTagSuccess) {
                                        mqttIndex = mqttIndex + 1;
                                        for (int i = 0; i < mqttMemberCountValue; i++) {
                                            byte groupMembersArray[] = new byte[8];
                                            for (int j = 0; j < 8; j++) {
                                                groupMembersArray[j] = origMqttMsgByte[mqttIndex + j];
                                            }
                                            long participants = ByteBuffer.wrap(groupMembersArray).order(ByteOrder.LITTLE_ENDIAN).getLong();
                                            mqttIndex = mqttIndex + 8;
                                            if (i == (mqttMemberCountValue - 1)) {
                                                mqttMembersValue = mqttMembersValue + participants;
                                            } else {
                                                mqttMembersValue = mqttMembersValue + participants + ",";
                                            }
                                        }
                                        processMTagSuccess = true;
                                    } else {
                                        break;
                                    }
                                } else if (mqttPacketValue == 0) {
                                    if (processLTagSuccess) {
                                        mqttIndex = mqttIndex + 1;
                                        if (mqttMsgLengthValue > 0) {
                                            byte messageArray[] = null;
                                            try {
                                                messageArray = new byte[mqttMsgLengthValue];
                                            } catch (Exception err) {
                                                err.printStackTrace();
                                                processMTagSuccess = false;
                                                break;
                                            }

                                            for (int i = 0; i < mqttMsgLengthValue; i++) {
                                                messageArray[i] = origMqttMsgByte[mqttIndex + i];
                                            }
                                            mqttMessageValue = new String(messageArray);
                                            processMTagSuccess = true;
                                            mqttIndex = mqttIndex + mqttMsgLengthValue + 1;
                                        }
                                    } else {
                                        break;
                                    }
                                }
                            }
                        } else if (mqttTag == 71) /* "G" */ {
                            mqttIndex = mqttIndex + 1;
                            byte groupIDArray[] = new byte[8];
                            for (int i = 0; i < 8; i++) {
                                groupIDArray[i] = origMqttMsgByte[mqttIndex + i];
                            }
                            mqttGroupIDValue = ByteBuffer.wrap(groupIDArray).order(ByteOrder.LITTLE_ENDIAN).getLong();
                            processGTagSuccess = true;
                            mqttIndex = mqttIndex + 8;
                        } else if (mqttTag == 83) /* "S" */ {
                            if (processLTagSuccess) {
                                mqttIndex = mqttIndex + 1;
                                if (mqttMsgLengthValue > 0) {
                                    byte subjectArray[] = null;
                                    try {
                                        subjectArray = new byte[mqttMsgLengthValue];
                                    } catch (Exception err) {
                                        err.printStackTrace();
                                        processSTagSuccess = false;
                                        break;
                                    }
                                    for (int i = 0; i < mqttMsgLengthValue; i++) {
                                        subjectArray[i] = origMqttMsgByte[mqttIndex + i];
                                    }
                                    mqttSubjectValue = new String(subjectArray);
                                    processSTagSuccess = true;
                                    mqttIndex = mqttIndex + mqttMsgLengthValue;
                                }
                            } else {
                                break;
                            }
                        } else if (mqttTag == 67) /* "C" */ {
                            mqttIndex = mqttIndex + 1;
                            mqttMemberCountValue = origMqttMsgByte[mqttIndex];
                            processCTagSuccess = true;
                            mqttIndex = mqttIndex + 1;
                        } else if (mqttTag == 78) /* "N" */ {
                            mqttIndex = mqttIndex + 1;
                            mqttAdminCountValue = origMqttMsgByte[mqttIndex];
                            processNTagSuccess = true;
                            mqttIndex = mqttIndex + 1;
                        } else if (mqttTag == 65) /* "A" */ {
                            if (processPTagSuccess) {
                                if (mqttPacketValue == -117) {
                                    if (processNTagSuccess) {
                                        mqttIndex = mqttIndex + 1;
                                        for (int i = 0; i < mqttAdminCountValue; i++) {
                                            byte groupAdminsArray[] = new byte[8];
                                            for (int j = 0; j < 8; j++) {
                                                groupAdminsArray[j] = origMqttMsgByte[mqttIndex + j];
                                            }
                                            long admins = ByteBuffer.wrap(groupAdminsArray).order(ByteOrder.LITTLE_ENDIAN).getLong();
                                            mqttIndex = mqttIndex + 8;
                                            if (i == (mqttAdminCountValue - 1)) {
                                                mqttAdminsValue = mqttAdminsValue + admins;
                                            } else {
                                                mqttAdminsValue = mqttAdminsValue + admins + ",";
                                            }
                                        }
                                        processATagSuccess = true;
                                    } else {
                                        break;
                                    }
                                }
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        if (!processVTagSuccess) {
            return;
        }


        PowerManager pm = (PowerManager) ctx.getSystemService(Context.POWER_SERVICE);
        boolean isScreenOn = pm.isScreenOn();
        if (isScreenOn == false) {
            WakeLock wl = pm.newWakeLock(PowerManager.FULL_WAKE_LOCK | PowerManager.ACQUIRE_CAUSES_WAKEUP | PowerManager.ON_AFTER_RELEASE, "MyLock");
            if (wl.isHeld()) {
                wl.release();
            }
            wl.acquire(10000);
            WakeLock wl_cpu = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "MyCpuLock");
            if (wl_cpu.isHeld()) {
                wl_cpu.release();
            }
            wl_cpu.acquire(10000);
        }

        String message = mqttMessageValue;
        Log.v("mqtt", "from: " + sender);
        Log.v("mqtt", "message: " + message);
        Intent intent = new Intent();
        intent.setClassName(ctx, "org.eclipse.paho.android.service.sample.MainActivity");
        intent.putExtra("handle", clientHandle);
        String ns = Context.NOTIFICATION_SERVICE;
        NotificationManager mNotificationManager = (NotificationManager) ctx.getSystemService(ns);
        int messageNotificationId = 1;
        mNotificationManager.cancel(messageNotificationId);
        Calendar.getInstance().getTime().toString();
        long when = System.currentTimeMillis();
        String ticker = sender + " " + mqttMessageValue;
        PendingIntent pendingIntent = PendingIntent.getActivity(ctx,
                3, intent, 0);
        NotificationCompat.Builder notificationCompat = new NotificationCompat.Builder(ctx);
        notificationCompat.setAutoCancel(true)
                .setContentTitle(sender)
                .setContentIntent(pendingIntent)
                .setContentText(mqttMessageValue)
                .setTicker(ticker)
                .setWhen(when)
                .setSmallIcon(R.drawable.ic_launcher);

      //  Notification notification = notificationCompat.build();
        Bitmap iconLarge = BitmapFactory.decodeResource(ctx.getResources(), R.drawable.ic_launcher);
        NotificationCompat.Builder mBuilder = new NotificationCompat.Builder(
                ctx)
                .setSmallIcon(R.drawable.ic_launcher)
                .setLargeIcon(iconLarge)
                .setContentTitle(sender)
                .setContentText(mqttMessageValue);

        mBuilder.setContentIntent(pendingIntent);
        mBuilder.setTicker(message);
        mBuilder.setAutoCancel(true);
        mBuilder.setDefaults(Notification.DEFAULT_SOUND | Notification.DEFAULT_VIBRATE | Notification.DEFAULT_LIGHTS);
        mNotificationManager.notify(messageNotificationId, mBuilder.build());
    }


    private long getMqttTag(byte mqttMsgByte[], int mqttIndexTagCounter) {
        try {
            long mTag = mqttMsgByte[mqttIndexTagCounter];
            return mTag;
        } catch (Exception err) {
            err.printStackTrace();
        }
        return -1;
    }

    /**
     * Store details of sent messages so we can handle "deliveryComplete"
     * callbacks from the mqttClient
     *
     * @param topic
     * @param msg
     * @param messageToken
     * @param invocationContext
     * @param activityToken
     */
    private void storeSendDetails(final String topic, final MqttMessage msg,
                                  final IMqttDeliveryToken messageToken,
                                  final String invocationContext, final String activityToken) {
        savedTopics.put(messageToken, topic);
        savedSentMessages.put(messageToken, msg);
        savedActivityTokens.put(messageToken, activityToken);
        savedInvocationContexts.put(messageToken, invocationContext);
    }

    /**
     * Acquires a partial wake lock for this client
     */
    private void acquireWakeLock() {
        if (wakelock == null) {
            PowerManager pm = (PowerManager) service
                    .getSystemService(Service.POWER_SERVICE);
            wakelock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK,
                    wakeLockTag);
        }
        wakelock.acquire();

    }

    /**
     * Releases the currently held wake lock for this client
     */
    private void releaseWakeLock() {
        if (wakelock != null && wakelock.isHeld()) {
            wakelock.release();
        }
    }

    /**
     * General-purpose IMqttActionListener for the Client context
     * <p>
     * Simply handles the basic success/failure cases for operations which don't
     * return results
     *
     */
    private class MqttConnectionListener implements IMqttActionListener {

        private final Bundle resultBundle;

        private MqttConnectionListener(Bundle resultBundle) {
            this.resultBundle = resultBundle;
        }

        @Override
        public void onSuccess(IMqttToken asyncActionToken) {
            Log.v("mqtt", "MqttConnection MqttConnectionListener onSuccess");
            service.callbackToActivity(clientHandle, Status.OK, resultBundle);
        }

        @Override
        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
            Log.v("mqtt", "MqttConnection MqttConnectionListener onFailure");
            resultBundle.putString(MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                    exception.getLocalizedMessage());

            resultBundle.putSerializable(
                    MqttServiceConstants.CALLBACK_EXCEPTION, exception);

            service.callbackToActivity(clientHandle, Status.ERROR, resultBundle);
        }
    }

    /**
     * Receive notification that we are offline<br>
     * if cleanSession is true, we need to regard this as a disconnection
     */
    void offline() {
        Log.v("mqtt", "MqttConnection offline()");
        if (!disconnected && !cleanSession) {
            Exception e = new Exception("Android offline");
            connectionLost(e);
        }
    }

    /**
     * Reconnect<br>
     * Only appropriate if cleanSession is false and we were connected.
     * Declare as synchronized to avoid multiple calls to this method to send connect
     * multiple times
     */
    synchronized void reconnect() {
        Log.v("mqtt", "MqttConnection reconnect()");
        if (isConnecting) {
            Log.v("mqtt", "MqttConnection reconnect() The client is connecting. Reconnect return directly.");
            service.traceDebug(TAG, "The client is connecting. Reconnect return directly.");
            return;
        }

        if (!service.isOnline()) {
            Log.v("mqtt", "The network is not reachable. Will not do reconnect");
            service.traceDebug(TAG,
                    "The network is not reachable. Will not do reconnect");
            return;
        }

        if (disconnected && !cleanSession) {
            Log.v("mqtt", "Do Real Reconnect!");
            // use the activityToke the same with action connect
            service.traceDebug(TAG, "Do Real Reconnect!");
            final Bundle resultBundle = new Bundle();
            resultBundle.putString(
                    MqttServiceConstants.CALLBACK_ACTIVITY_TOKEN,
                    reconnectActivityToken);
            resultBundle.putString(
                    MqttServiceConstants.CALLBACK_INVOCATION_CONTEXT, null);
            resultBundle.putString(MqttServiceConstants.CALLBACK_ACTION,
                    MqttServiceConstants.CONNECT_ACTION);

            try {

                IMqttActionListener listener = new MqttConnectionListener(resultBundle) {
                    @Override
                    public void onSuccess(IMqttToken asyncActionToken) {
                        // since the device's cpu can go to sleep, acquire a
                        // wakelock and drop it later.
                        service.traceDebug(TAG, "Reconnect Success!");
                        service.traceDebug(TAG, "DeliverBacklog when reconnect.");
                        doAfterConnectSuccess(resultBundle);
                    }

                    @Override
                    public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                        resultBundle.putString(
                                MqttServiceConstants.CALLBACK_ERROR_MESSAGE,
                                exception.getLocalizedMessage());
                        resultBundle.putSerializable(
                                MqttServiceConstants.CALLBACK_EXCEPTION,
                                exception);
                        service.callbackToActivity(clientHandle, Status.ERROR,
                                resultBundle);

                        doAfterConnectFail(resultBundle);

                    }
                };

                myClient.connect(connectOptions, null, listener);
                setConnectingState(true);
            } catch (MqttException e) {
                service.traceError(TAG, "Cannot reconnect to remote server." + e.getMessage());
                setConnectingState(false);
                handleException(resultBundle, e);
            }
        }
    }

    /**
     *
     * @param isConnecting
     */
    synchronized void setConnectingState(boolean isConnecting) {
        Log.v("mqtt", "MqttConnection setConnectingState");
        this.isConnecting = isConnecting;
    }


}
