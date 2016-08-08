package org.eclipse.paho.android.service.sample;

import android.app.ActivityManager;
import android.content.Context;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import org.eclipse.paho.android.service.NanuService;
import org.eclipse.paho.client.mqttv3.IMqttToken;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Map;

/**
 * Created by fritzllanora on 4/8/16.
 */
public class MainActivity extends AppCompatActivity {

    private ChangeListener changeListener = new ChangeListener();
    private Button btnPublish;
    private Button btnClear;
    private EditText edtMessage;
    private EditText edtNumber;

    private static StartNanuServiceTask startNanuServiceTask;

    public void initComponents() {
        btnPublish = (Button) findViewById(R.id.btnPublish);
        btnClear = (Button) findViewById(R.id.btnClear);
        edtMessage = (EditText) findViewById(R.id.edtMessage);
        edtNumber = (EditText) findViewById(R.id.edtNumber);
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        initComponents();

        if (startNanuServiceTask == null) {
            startNanuServiceTask = new StartNanuServiceTask(new Intent(MainActivity.this, NanuService.class));
            startNanuServiceTask.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "");
        } else {
            if (startNanuServiceTask.getStatus().equals(AsyncTask.Status.RUNNING)) {
            } else {
                startNanuServiceTask.cancel(true);
                startNanuServiceTask = new StartNanuServiceTask(new Intent(MainActivity.this, NanuService.class));
                startNanuServiceTask.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "");
            }
        }



        btnPublish.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                edtMessage.setText("");
                edtNumber.setText("");
            }
        });

        btnPublish.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String message = edtMessage.getText().toString();
                String recipientNumber = edtNumber.getText().toString();
                if ((message.length() > 0)&&(recipientNumber.length()>0)) {
                    Log.v("mqtt", "Message: " + message);
                    Log.v("mqtt", "recipientNumber: " + recipientNumber);
                    NanuService.publishMqttMessage(MainActivity.this, message, recipientNumber, new OnPublishListener() {
                        @Override
                        public void onSuccess(IMqttToken asyncActionToken) {
                            edtMessage.setText("");
                            edtNumber.setText("");
                            Toast.makeText(MainActivity.this, "Publish success",
                                    Toast.LENGTH_SHORT).show();
                        }

                        @Override
                        public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                            Toast.makeText(MainActivity.this, "Publish fail",
                                    Toast.LENGTH_SHORT).show();
                        }
                    });
                }
            }
        });

       if (isMyServiceRunning(MainActivity.this, NanuService.class)) {
            Log.v("mqtt", "MainActivity onCreate NanuService is running");
           NanuService.checkMqttConnection(MainActivity.this);
        } else {
            Log.v("mqtt", "MainActivity onCreate NanuService is not running");
        }
    }

    public boolean isMyServiceRunning(Context ctx, Class<?> serviceClass) {
        ActivityManager manager = (ActivityManager) ctx.getSystemService(Context.ACTIVITY_SERVICE);
        for (ActivityManager.RunningServiceInfo service : manager.getRunningServices(Integer.MAX_VALUE)) {
            if (serviceClass.getName().equals(service.service.getClassName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void onResume() {
        super.onResume();
        Log.v("mqtt", "MainActivity onResume()");
        //Recover connections.
        Map<String, Connection> connections = Connections.getInstance(MainActivity.this).getConnections();
        //Register receivers again
        for (Connection connection : connections.values()) {
            connection.getClient().registerResources(MainActivity.this);
            connection.getClient().setCallback(new MqttCallbackHandler(MainActivity.this, connection.getClient().getServerURI() + connection.getClient().getClientId()));
        }
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        Log.v("mqtt", "MainActivity onDestroy()");
        Map<String, Connection> connections = Connections.getInstance(MainActivity.this).getConnections();

        for (Connection connection : connections.values()) {
            connection.registerChangeListener(changeListener);
            connection.getClient().unregisterResources();
        }
    }

    private class ChangeListener implements PropertyChangeListener {
        /**
         * @see PropertyChangeListener#propertyChange(PropertyChangeEvent)
         */
        @Override
        public void propertyChange(PropertyChangeEvent event) {
            Log.v("mqtt", "MainActivity ChangeListener propertyChange()");
        }

    }


    class StartNanuServiceTask extends AsyncTask<String, String, String> {
        Intent intent;

        public StartNanuServiceTask(Intent intent) {
            this.intent = intent;
        }

        @Override
        protected String doInBackground(String... uri) {
            startService(intent);
            return null;
        }

        @Override
        protected void onPostExecute(String result) {
            super.onPostExecute(result);
        }
    }

}
