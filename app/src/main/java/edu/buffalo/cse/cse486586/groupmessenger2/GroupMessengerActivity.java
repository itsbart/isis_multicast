package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.Context;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */

public class GroupMessengerActivity extends Activity {


    static final int SERVER_PORT = 10000;

    static final String[] REMOTE_PORTS = new String[]{null,
            "11108",    //process id 1
            "11112",    //process id 2
            "11116",    //..
            "11120",
            "11124"};


    //used in send as seqnum
    static int msgCounter = 0;
    static int nextMsgID = -1;

    //storing heartbeat timestamps
    static long[] heartbeep = new long[6];

    //managing incoming messages
    MessageManager messageManager = new MessageManager();

    private ScheduledExecutorService scheduler;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.i("MyINFO", "PORT: " + myPort);

        for (int i = 1; i <= 5; i++) {
            if (REMOTE_PORTS[i].equals(myPort)) {
                //set process id && next msg id for sender
                messageManager.process_id = i;
                nextMsgID = i;
            }
        }

        messageManager.setContext(this);

        try {

            /* listen and handle incoming msgs in FIFO-TOTAL order */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.i("MyINFO", "SERVER SOCKET: " + serverSocket.getInetAddress());
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            e.printStackTrace();
        }

        final Button send_button = (Button) findViewById(R.id.button4);
        final EditText editText = (EditText) findViewById(R.id.editText1);

        send_button.setOnClickListener(new View.OnClickListener() {

            @Override
            public void onClick(View v) {

                String message = editText.getText().toString();
                editText.setText(""); //reset

                if (message.length() < 1 || message.isEmpty()) {
                    tv.append("EMPTY STRING - NO SEND\n");
                } else {
                    message += "\n";
                    tv.append("SENT: \t" + message);
                    new BroadcastTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, myPort);
                }

            }
        });

        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {

                for (int i = 1; i <= 5; i++) {

                    try {

                        if (i != messageManager.process_id) {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(REMOTE_PORTS[i]));

                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            out.println("BEEP:" + messageManager.process_id);
                            socket.close();
                        }

                    } catch (IOException e) {
                        Log.i("EXCEPTION", "IO SOCKET");
                        //e.printStackTrace();
                    }

                }

            }
        }, 0, 1, TimeUnit.SECONDS);

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }


    //Message Handler - Listener
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket ss = sockets[0];
            Socket connection;

            try {

                while (true) {

                    connection = ss.accept();

                    //timestamp of msg arrival
                    long ts = System.currentTimeMillis();

                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(connection.getInputStream()));

                    String response;
                    while ((response = in.readLine()) != null) if (response.length() > 0) {

                        String[] tokens = response.split(":");

                        if (tokens[0].equals("BEEP")) {
                            int receiver = Integer.parseInt(tokens[1]);
                            heartbeep[receiver] = ts;

                            //Scan for timeouts - potential failure
                            long prev;
                            for (int i = 1; i <= 5; i++) {
                                prev = heartbeep[i];

                                if (ts - prev > 6500 && i != messageManager.process_id &&
                                        prev != 0 && i != receiver) {

                                    Log.w("WARNING", "BEEP TIMEOUT DETECTED:" + (ts - prev));
                                    Log.w("WARNING", "NODE: " + i + " FAILED! ");

                                    messageManager.handleFailure(i);
                                }
                            }

                        } else {
                            messageManager.handleMessage(response, ts);
                        }

                        publishProgress(new String[]{response, ts + ""});
                    }

                    connection.close();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }

        @Override
        protected void onProgressUpdate(String... strings) {

            if (strings[0] != null && strings[1] != null) {

                String strReceived = strings[0].trim();
                String[] tokens = strReceived.split(":");

                if (tokens[0].equals("MESSAGE") && tokens.length > 3) {
                    TextView tv = (TextView) findViewById(R.id.textView1);
                    tv.append(tokens[3] + "\t\n");
                }

                //release messages from dispatch queue
                messageManager.releaseMessages();
            }
        }
    }


    //Message Broadcaster
    private class BroadcastTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... params) {

            String msgToSend = params[0];

            // [HEADER]:[MSG_SEQNUM]:[MSG_ID]:[MESSAGE]
            String encodedMsg = "MESSAGE:" + ++msgCounter + ":" + nextMsgID + ":" + msgToSend;

            //add message instantly (proposals can arrive before message itself)
            messageManager.initNewMessage(encodedMsg.split(":"), System.currentTimeMillis() + 10);

            try {

                for (int i = 1; i <= 5; i++) {

                    Log.i("MyINFO", "DISPATCH TO: " + REMOTE_PORTS[i]);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORTS[i]));

                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(encodedMsg);
                    socket.close();

                }

            } catch (UnknownHostException e) {
                Log.e("MyINFO", "BroadcastTask UnknownHostException");
                e.printStackTrace();
            } catch (SocketTimeoutException e) {
                Log.e("MyINFO", "Timeout exception");
            } catch (SocketException e) {
                Log.e("MyINFO", "General Socket Exception");
            } catch (IOException e) {
                Log.e("MyINFO", "BroadcastTask socket IOException");
                e.printStackTrace();
            }

            //offset by 5 hence we get unique msg_id for 5 process id's
            nextMsgID += 5;

            return null;
        }
    }

}

