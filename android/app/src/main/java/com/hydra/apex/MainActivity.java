package com.hydra.apex;

import android.app.Activity;
import android.os.Bundle;
import android.content.Intent;
import android.net.Uri;
import android.widget.*;

public class MainActivity extends Activity {
    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        setContentView(R.layout.activity_main);
        EditText url = findViewById(R.id.backendUrl);
        Button connect = findViewById(R.id.connect);
        connect.setOnClickListener(v -> {
            String value = url.getText().toString().trim();
            if (value.isEmpty()) value = "http://127.0.0.1:8181";
            try { startActivity(new Intent(Intent.ACTION_VIEW, Uri.parse(value))); }
            catch (Exception e) { Toast.makeText(this, "Invalid backend URL", Toast.LENGTH_SHORT).show(); }
        });
    }
}
