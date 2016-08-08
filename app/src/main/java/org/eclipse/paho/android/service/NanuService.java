package org.eclipse.paho.android.service;

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.os.IBinder;
import android.os.PowerManager;
import android.os.SystemClock;
import android.support.annotation.Nullable;
import android.util.Log;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.RadioGroup;

import org.eclipse.paho.android.service.sample.ActionListener;
import org.eclipse.paho.android.service.sample.ActivityConstants;
import org.eclipse.paho.android.service.sample.Connection;
import org.eclipse.paho.android.service.sample.Connections;
import org.eclipse.paho.android.service.sample.MainActivity;
import org.eclipse.paho.android.service.sample.MqttCallbackHandler;
import org.eclipse.paho.android.service.sample.MqttTraceCallback;
import org.eclipse.paho.android.service.sample.OnPublishListener;
import org.eclipse.paho.android.service.sample.R;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Created by fritzllanora on 4/8/16.
 */
public class NanuService extends Service {

    private static ChangeListener changeListener;
    private static Context sContext;
    private WeakReference sContextWeakReference;

    @Override
    public void onCreate() {
        Log.v("mqtt", "NanuService onCreate()");
        super.onCreate();
        sContextWeakReference = new WeakReference<>(getApplicationContext());
        sContext = (Context) sContextWeakReference.get();
        connectToMqtt();
    }

    public static Context getContext() {
        return sContext;
    }

    public static void connectToMqtt()
    {
        Map<String, Connection> connections = Connections.getInstance(NanuService.getContext()).getConnections();
        for (Connection connection : connections.values()){
            connection.registerChangeListener(changeListener);
            connection.getClient().unregisterResources();
        }
        changeListener = new ChangeListener();
        MqttConnectOptions conOpt = new MqttConnectOptions();
        String server = "m.hellonanu.com";
        String clientId = "6587777777";
        int port = 443;
        boolean cleanSession = false;
        boolean ssl = true;
        String ssl_key = "";
        String uri = null;
        if (ssl) {
            Log.e("SSLConnection", "Doing an SSL Connect");
            uri = "ssl://";
        }
        else {
            uri = "tcp://";
        }
        uri = uri + server + ":" + port;
        MqttAndroidClient client;
        client = Connections.getInstance(NanuService.getContext()).createClient(NanuService.getContext(), uri, clientId);
        if (ssl){
            try {
                if(ssl_key != null && !ssl_key.equalsIgnoreCase(""))
                {
                    FileInputStream key = new FileInputStream(ssl_key);
                    conOpt.setSocketFactory(client.getSSLSocketFactory(key,
                            "mqtttest"));
                }
            } catch (MqttSecurityException e) {
                Log.e(NanuService.getContext().getClass().getCanonicalName(),
                        "MqttException Occured: ", e);
            } catch (FileNotFoundException e) {
                Log.e(NanuService.getContext().getClass().getCanonicalName(),
                        "MqttException Occured: SSL Key file not found", e);
            }
        }
        String clientHandle = uri + clientId;
        String message = "";
        String topic = "";
        Integer qos = 1;
        Boolean retained = false;
        String username = "6587777777";
        String password = "470311";
        int timeout = 60;
        int keepalive = 100;
        Connection connection = new Connection(clientHandle, clientId, server, port,
                NanuService.getContext(), client, ssl);
        connection.registerChangeListener(changeListener);
        String[] actionArgs = new String[1];
        actionArgs[0] = clientId;
        connection.changeConnectionStatus(Connection.ConnectionStatus.CONNECTING);
       conOpt.setCleanSession(cleanSession);
        conOpt.setConnectionTimeout(timeout);
        conOpt.setKeepAliveInterval(keepalive);
        if (!username.equals(ActivityConstants.empty)) {
            conOpt.setUserName(username);
        }
        if (!password.equals(ActivityConstants.empty)) {
            conOpt.setPassword(password.toCharArray());
        }
        final ActionListener callback = new ActionListener(NanuService.getContext(),
                ActionListener.Action.CONNECT, clientHandle, actionArgs);
        boolean doConnect = true;
        if ((!message.equals(ActivityConstants.empty))
                || (!topic.equals(ActivityConstants.empty))) {
            try {
                conOpt.setWill(topic, message.getBytes(), qos.intValue(),
                        retained.booleanValue());
            }
            catch (Exception e) {
                Log.e(NanuService.getContext().getClass().getCanonicalName(), "Exception Occured", e);
                doConnect = false;
                callback.onFailure(null, e);
            }
        }
        client.setCallback(new MqttCallbackHandler(NanuService.getContext(), clientHandle));
        client.setTraceCallback(new MqttTraceCallback());
        connection.addConnectionOptions(conOpt);
        Connections.getInstance(NanuService.getContext()).addConnection(connection);
        if (doConnect) {
            try {
                client.connect(conOpt, null, callback);
            }
            catch (MqttException e) {
                Log.e(NanuService.getContext().getClass().getCanonicalName(),
                        "MqttException Occured", e);
            }
        }
    }

    public static void publishMqttMessage(Context ctx, String message, String number, final OnPublishListener onPublishListener)
    {
        Log.v("mqtt","NanuService publishMqttMessage()");
        String recipientNumber = number.replaceAll("[^0-9\\s]", "");
        recipientNumber = recipientNumber.replaceAll("\\s+", "");
        String server = "m.hellonanu.com";
        String clientId = "6587777777";
        int port = 443;
        boolean cleanSession = false;
        boolean ssl = true;
        String uri = null;
        if (ssl) {
            Log.e("SSLConnection", "Doing an SSL Connect");
            uri = "ssl://";
        }
        else {
            uri = "tcp://";
        }
        uri = uri + server + ":" + port;
        String clientHandle = uri + clientId;
        String topic = "dbox.usr." + recipientNumber;
        int qos = 1;
        boolean retained = false;

        byte[] messageContentByteArray;
        int messageContentSize = 0;
        try {
            messageContentByteArray = message.getBytes("UTF-8");
            messageContentSize = messageContentByteArray.length;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        int totalMessageSize = 1 + 1 + 1 + 8 + 1 + 1 + 1 + 4 + 1 + messageContentSize;
        byte finalMessage[] = new byte[totalMessageSize];
        finalMessage[0] = 86; /* "V" */
        finalMessage[1] = 0; /* 0 */
        finalMessage[2] = 84; /* "T" */
        long timeStampInt = System.currentTimeMillis();
        byte timestampSizeByteArray[] = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(timeStampInt).array();
        for (int i = 0; i < timestampSizeByteArray.length; i++) {
            finalMessage[3 + i] = timestampSizeByteArray[i];
        }
        MqttMessage msg = new MqttMessage(message.getBytes());
        msg.setQos(1);
        msg.setRetained(false);
        finalMessage[11] = 80; /* "P" */
        finalMessage[12] = 0;
        finalMessage[13] = 76; /* "L" */
        byte messageContentSizeByteArray[] = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(messageContentSize).array();
        int startingMessageSizeIndex = 0;
        for (int i = 0; i < messageContentSizeByteArray.length; i++) {
            finalMessage[14 + i] = messageContentSizeByteArray[i];
            startingMessageSizeIndex++;
        }
        try {
            finalMessage[18] = 77; /* "M" */
            byte[] messageContentByteStringArray = message.getBytes("UTF-8");
            for (int i = 0; i < messageContentByteStringArray.length; i++) {
                finalMessage[19 + i] = messageContentByteStringArray[i];
                startingMessageSizeIndex++;
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String messageDate = sdf.format(timeStampInt);
        String messageSentBody = message;
        String messageSentDate = messageDate;
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setPayload(finalMessage);
        mqttMessage.setQos(qos);
        mqttMessage.setRetained(false);

        try {
            Connections.getInstance(ctx).getConnection(clientHandle).getClient().publish(topic,mqttMessage,ctx, new IMqttActionListener(){
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    Log.v("mqtt","NanuService publishMqttMessage() publish success");
                    onPublishListener.onSuccess(asyncActionToken);
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    Log.v("mqtt","NanuService publishMqttMessage() publish fail");
                    onPublishListener.onFailure(asyncActionToken, exception);
                }
            });

        }
        catch (MqttSecurityException e) {
        }
        catch (MqttException e) {
        }

    }

    public static void checkMqttConnection(Context ctx)
    {
        Log.v("mqtt","NanuService checkMqttConnection()");
        String server = "m.hellonanu.com";
        String clientId = "6587777777";
        int port = 443;
        boolean cleanSession = false;
        boolean ssl = true;
        String uri = null;
        if (ssl) {
            Log.e("SSLConnection", "Doing an SSL Connect");
            uri = "ssl://";
        }
        else {
            uri = "tcp://";
        }
        uri = uri + server + ":" + port;
        String clientHandle = uri + clientId;

        try {
            if(Connections.getInstance(ctx).getConnection(clientHandle).getClient()!=null)
            {
                if(!Connections.getInstance(ctx).getConnection(clientHandle).getClient().isConnected())
                {
                    connectToMqtt();
                }
            }
        }
        catch (Exception e) {
        }
//        catch (MqttSecurityException e) {
//        }
//        catch (MqttException e) {
//        }

    }

    public static void processIncomingMessages(Context ctx)
    {
        PowerManager pm = (PowerManager) ctx.getSystemService(Context.POWER_SERVICE);
        boolean isScreenOn = pm.isScreenOn();
        if (isScreenOn == false) {
            PowerManager.WakeLock wl = pm.newWakeLock(PowerManager.FULL_WAKE_LOCK | PowerManager.ACQUIRE_CAUSES_WAKEUP | PowerManager.ON_AFTER_RELEASE, "MyLock");
            if (wl.isHeld()) {
                wl.release();
            }
            wl.acquire(10000);
            PowerManager.WakeLock wl_cpu = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "MyCpuLock");
            if (wl_cpu.isHeld()) {
                wl_cpu.release();
            }
            wl_cpu.acquire(10000);
        }
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        Log.v("mqtt", "NanuService onStartCommand()");
        sContextWeakReference = new WeakReference<>(getApplicationContext());
        sContext = (Context) sContextWeakReference.get();
        return Service.START_STICKY;
    }

    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        Log.v("mqtt", "NanuService onBind()");
        return null;
    }

    @Override
    public boolean onUnbind(Intent intent) {
        Log.v("mqtt", "NanuService onUnbind()");
        return false;
    }

    @Override
    public void onDestroy() {
        Log.v("mqtt", "NanuService onDestroy()");
        Map<String, Connection> connections = Connections.getInstance(NanuService.this).getConnections();

        for (Connection connection : connections.values()){
            connection.registerChangeListener(changeListener);
            connection.getClient().unregisterResources();
        }
        super.onDestroy();
    }

    @Override
    public void onRebind(Intent intent) {
        Log.v("mqtt", "NanuService onRebind()");
        connectToMqtt();
    }

    @Override
    public void onTaskRemoved(Intent rootIntent) {
        Log.v("mqtt","NanuService onTaskRemoved()");
        Intent restartServiceIntent = new Intent(getApplicationContext(), this.getClass());
        restartServiceIntent.setPackage(getPackageName());

        PendingIntent restartServicePendingIntent = PendingIntent.getService(getApplicationContext(), 1, restartServiceIntent, PendingIntent.FLAG_ONE_SHOT);
        AlarmManager alarmService = (AlarmManager) getApplicationContext().getSystemService(Context.ALARM_SERVICE);
        alarmService.set(AlarmManager.ELAPSED_REALTIME,
                SystemClock.elapsedRealtime() + 1000,
                restartServicePendingIntent);
        super.onTaskRemoved(rootIntent);
    }

    private static class ChangeListener implements PropertyChangeListener {

        /**
         * @see PropertyChangeListener#propertyChange(PropertyChangeEvent)
         */
        @Override
        public void propertyChange(PropertyChangeEvent event) {
            Log.v("mqtt","ClientConnections ChangeListener propertyChange()");
        }

    }
}
