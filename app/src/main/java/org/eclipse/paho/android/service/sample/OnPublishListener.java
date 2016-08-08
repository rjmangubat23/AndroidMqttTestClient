package org.eclipse.paho.android.service.sample;

import org.eclipse.paho.client.mqttv3.IMqttToken;

public interface OnPublishListener {
    void onSuccess(IMqttToken asyncActionToken);
    void onFailure(IMqttToken asyncActionToken, Throwable exception);
}
