package edu.buffalo.cse.cse486586.groupmessenger2;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Bart Karmilowicz on 3/2/15
 * CSE 586 - FIFO:Total Ordering Multicast
 *
 * Abstraction created for managing incoming messages.
 * The main idea is to use 3 entities to support fifo, total ordering:
 *       - holdback queue for out of order messages (fifo order)
 *       - staging area for messages that have "unagreed" priority
 *       - priority queue (min heap) with msgs that have agreed priorities
 *
 *
 *       Handling 3 types of messages:
 *       - MESSAGE: send by a peer, carries a content
 *       - PROPOSAL: send by a peer to message owner after receiving a message
 *                      with proposed priority
 *       - AGREEMENT: send by a message owner to all processes after selecting MAX
 *                          priority from received proposals
 *
 *
 *      To properly handle each message, headers have been employed:
 *      - [MESSAGE][SEQ_NUM][MSG_ID][MESSAGE]
 *      - [PROPOSAL][MSG_ID][PROPOSED_PRIORITY]
 *      - [AGREED][MSG_ID][AGREED_PRIORITY]
 *
 * Decision to flush down already ordered messages (in min heap) is made when:
 *          - holdback queue does not have any buffered messages (empty)
 *          - staging area does not have any messages waiting for proposals / agreements (empty)
 *
 * If above conditions are met, all messages are delivered.
 *
 *
 * Failure Detection:
 *          - every 1 second each host multicasts "beep" - small msg with its id
 *          - every host keeps track of last updates from everyone
 *          - if beep has not been received for longer than 6s - timeout occurs
 *                  (i'm being generous here)
 *          - on timeout, we decrement number of node alive and we clean up all
 *              the state corresponding to failed node
 *
 *
 */

public class MessageManager {

    protected Context context;

    //process id from 1-5 {5 emulators}
    public int process_id = 0;

    //failure detection bookkeeping
    public int PEERS_EXPECTED = 5;
    public int[] peers_alive = new int[6];

    //global priority number local to this process
    public int global_priority = 0;

    //stores out of order msgs
    public LinkedList<Msg> holdback =
            new LinkedList<>();

    //unagreed priority messages KEY: MSG_ID , VALUE: Msg
    public Hashtable<Integer, Msg> staging =
            new Hashtable<>();

    //ordered messages by priority from smallest - highest
    public PriorityQueue<Msg> ordered =
          new PriorityQueue<>(10, new Comparator<Msg>() {

              @Override
              public int compare(Msg lhs, Msg rhs) {

                  if(lhs.agreed_proposal < rhs.agreed_proposal){
                      return -1;
                  }

                  if(rhs.agreed_proposal < lhs.agreed_proposal){
                      return 1;
                  }

                  return 0;
              }
          });

    //keeping track of expected sequence numbers for messages - FIFO
    public int[] expectedSeqs
            = new int[6];

    //saving messages - key for content values
    int globalKeyCounter = 0;

    ReentrantLock lock = new ReentrantLock();
    public LinkedList<String[]> dispatchQueue = new LinkedList();


    /**
    * Handles newly received message
    * Based on message header, calls appropriate subroutine
    *
    */

    public void handleMessage(String message, Long ts){

        String[] tokens = message.split(":");
        String header = tokens[0];

        switch(header){

            case "PROPOSAL" :
                //Log.i("MM", "PROPOSAL CASE");
                handleProposal(tokens);
                break;

            case "AGREED" :
                //Log.i("MM", "AGREEMENT CASE");
                handleAgreement(tokens);
                break;

            case "MESSAGE" :
                //Log.i("MM", "NEW MESSAGE CASE");
                initNewMessage(tokens, ts);
                break;

            default :
                throw new IllegalArgumentException("Un-recognizable message header: " + header);

        }

        flushMessagesInOrder();

    }

    /**
     * Method responsible for handing messages in sorted order
     * to ContentProvider.
     * Messages can only by delivered when queue and staging map
     * are empty.
     */

    public synchronized void flushMessagesInOrder(){

        if(!holdback.isEmpty() || !staging.isEmpty() ||
                ordered.isEmpty()){
            return;
        }

        Log.i("DLV", "FLUSHING ORDERED QUEUE TO DISK");
        ContentValues[] values = new ContentValues[ordered.size()];
        ContentResolver mContentResolver = context.getContentResolver();
        Uri mUri = Uri.parse("content://" + "edu.buffalo.cse.cse486586.groupmessenger2.provider");


        int i = 0;
        while(!ordered.isEmpty()){

            Msg root = ordered.poll();
            values[i] = new ContentValues();
            values[i].put("key", globalKeyCounter + "");
            values[i].put("value", root.message);
            Log.i("DLV", "ORDER: " + globalKeyCounter + " MESSAGE: " + root.message +
                    " PRIORITY: " + root.agreed_proposal);
            i++;
            globalKeyCounter++;
        }

        for(ContentValues cv : values){
            mContentResolver.insert(mUri, cv);
        }

    }

    /**
     * Method responsible for handling agreement from message owner
     * Updates messages with un-agreed priority and moves them to ordered queue
     *
     */

    public synchronized void handleAgreement(String[] msg){

        //validate arguments
        int msgid = Integer.parseInt(msg[1]);
        double agreement = Double.parseDouble(msg[2]);

        Log.i("AGREED", "MESSAGE ID:" + msgid + " " + "AGREEMENT:" + agreement);

        if(staging.containsKey(msgid)){
            staging.get(msgid).agreed_proposal = agreement;
            ordered.add(staging.remove(msgid));
        }

    }

    /**
     *  Method responsible for handling proposals from other nodes
     *  Updates messages with MAX priority received from other peers
     *  If all peers responded, moves message to ordered queue.
     *
     */

    public synchronized void handleProposal(String[] msg){

        //validate arguments

        int sender_id = Integer.parseInt(msg[1]);
        int msgid = Integer.parseInt(msg[2]);
        double proposal = Double.parseDouble(msg[3]);

        Log.i("PROPOSAL", "MESSAGE ID:" + msgid + " " + "PROPOSAL:" + proposal);

        if(staging.containsKey(msgid)){
            if(staging.get(msgid).agreed_proposal < proposal){
                Log.i("MM", "New MAX proposal:" + proposal);
                staging.get(msgid).agreed_proposal = proposal;
            }

            staging.get(msgid).peers_responded += 1;
            staging.get(msgid).proposals_recv[sender_id] = 1;

            Log.i("PROPOSAL", "MISSING:" + (PEERS_EXPECTED - staging.get(msgid).peers_responded));

            //if 5 peers responded - broadcast agreement
            if(checkAgreement(msgid)){
                staging.remove(msgid);
            }

        }

    }

    /**
     * Failure Handler.
     * Marks node as inactive and calls clean up method
     * which deletes all of the data corresponding to failed node.
     */

    public synchronized void handleFailure(int id){
        if (peers_alive[id] == 0) {
            PEERS_EXPECTED--;
            peers_alive[id] = 1;
            cleanup(id);
        }
    }

    /**
     * Cleans all the messages that were send by
     * process with given id.
     *
     */

    public synchronized void cleanup(int id){

        //clean holdback queue
        LinkedList<Msg> q = new LinkedList<>();

        for(Msg m : holdback){
            if(m.sender_id != id){
                q.add(m);
            }
        }

        holdback = q;

        //clean staging area
        Hashtable<Integer, Msg> m = new Hashtable();

        int before = staging.size();

        for(Integer key : staging.keySet()){
            if(staging.get(key).sender_id != id){
                if(!checkAgreement(key)){
                    m.put(key, staging.get(key));
                }
            }
        }

        staging = m;

        Log.i("SWEEP", "CLEANING COMPLETED. REMOVED: " + (before - m.size()));
        flushMessagesInOrder();
    }

    /**
     * Checks if everybody responded with proposal.
     * If that's the case, moves message to ordered queue
     * and multicasts agreed priority to everybody.
     */

    public synchronized boolean checkAgreement(int msgid){

        // potentially change for failure detection
        if(staging.get(msgid).peers_responded == PEERS_EXPECTED){
            Log.i("MM", "All peers responded with proposal");
            Log.i("MM", "Agreed Priority:" + staging.get(msgid).agreed_proposal);
            dispatchAgreement(msgid, staging.get(msgid).agreed_proposal);
            ordered.add(staging.get(msgid));
            return true;
        }

        return false;
    }

    /**
     * Creates new instance for message and adds it either to:
     *      - staging map if its in order
     *      - queue if its out of order
     */

    public synchronized void initNewMessage(String[] msg, long ts){

        //validate arguments
        int msgid = Integer.parseInt(msg[2]);

        //check if it's this process message..
        if(staging.containsKey(msgid)){
            staging.get(msgid).peers_responded += 1;

            if(checkAgreement(msgid)){
                staging.remove(msgid);
            }

            return;
        }

        Msg m = new Msg();

        m.seqnum = Integer.parseInt(msg[1]);
        m.msg_id = msgid;
        m.ts = ts;

        //decode sender id from msgid
        m.sender_id = m.msg_id % 5 > 0 ? m.msg_id % 5 : 5;
        Log.i("MM", "SENDER ID: " + m.sender_id);

        if(m.sender_id == process_id){
            m.msgOwner = true;

        }

        if(msg.length > 3) {
            m.message = msg[3];
        }

        //check if message arrived in order
        if(expectedSeqs[m.sender_id] + 1 == m.seqnum){
            global_priority += 1;
            m.agreed_proposal = global_priority;

            staging.put(m.msg_id, m);
            expectedSeqs[m.sender_id] += 1;

            if(!m.msgOwner) {
                dispatchProposal(m.sender_id, m.msg_id);
            }

            //check out of order messages
            scanHoldBackList();

        }else{
            Log.i("MM", "Message out of order, buffering..");
            holdback.add(m);
        }

    }

    /**
     * Called on UI thread, sends buffered messages
     * (proposals, agreements).
     */

    public void releaseMessages(){

        lock.lock();

        while(!dispatchQueue.isEmpty()){
            String[] args = dispatchQueue.remove();
            new MessageTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, args);
        }

        lock.unlock();

    }

    public void dispatchAgreement(int msgid, double agreement){

        lock.lock();
        dispatchQueue.add(new String[] {"AGREED", msgid+"", agreement+""});
        lock.unlock();

    }

    public void dispatchProposal(int id, int msgid){

        lock.lock();
        double proposal = global_priority + (process_id / 10.0);
        dispatchQueue.add(new String[]{"PROPOSAL", id+"", msgid+"", proposal+""});
        lock.unlock();

    }

    /**
     * Scans holdback queue to check if any messages
     * can be delivered in order.
     */

    public void scanHoldBackList(){

        for(int i = 0; i < holdback.size(); i++){
            Msg k = holdback.get(i);
            if(expectedSeqs[k.sender_id] + 1 == k.seqnum){
                staging.put(k.msg_id, k);

                dispatchProposal(k.sender_id, k.msg_id);

                holdback.remove(i);
                i--;
            }
        }

    }

   /**
    *
    * AsyncTask used to dispatch Proposals and Agreements
    * Handles two type of messages:
    * (1) sending AGREEMENT to all peers
    * (2) sending PROPOSAL to one peer
    * Important: Different Arguments are expected based on specified operation
    *
    */

    public class MessageTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... arguments) {

            ArrayList<String> destination = new ArrayList<String>();
            String message = null;

            String operation = arguments[0];

            //Expected ARGS: [PROPOSAL][RECEIVER_PROCESS_ID][MSG_ID][PROPOSED PRIORITY]
            if(operation.equals("PROPOSAL")){
                int receiverID = Integer.parseInt(arguments[1]);

                destination.add(GroupMessengerActivity.REMOTE_PORTS[receiverID]);

                if(arguments[2] != null){
                    message = "PROPOSAL:" + process_id + ":" + arguments[2] + ":" + arguments[3];
                }

            //Expected ARGS: [AGREED][MSGID][AGREEMENT]
            }else if(operation.equals("AGREED")){

                //prepare receivers - everyone except this process
                for(int i = 1; i <= 5; i++){
                    if(i != process_id)
                        destination.add(GroupMessengerActivity.REMOTE_PORTS[i]);
                }

                if(arguments[1] != null){
                    message = "AGREED:" + arguments[1] + ":" + arguments[2];
                }

            }else{
                throw new IllegalArgumentException("unresolved operation" + operation);
            }

            if(message == null || destination.size() < 1){
                Log.i("ERROR", "MessageTask error");
            }


            //Dispatch messages
            for(String port : destination){

                try {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));

                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(message);
                    socket.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

            return null;
        }
    }

    public void setContext(Context context){
        this.context = context.getApplicationContext();
    }

    /* Dummy Information holder for each received message */
    public class Msg {

        public String message;
        public long ts;
        public int msg_id = -1;
        public int sender_id = -1;
        public int seqnum = -1;
        public boolean msgOwner = false;
        public int peers_responded = 0;
        public int[] proposals_recv = new int[6];
        public double agreed_proposal = -1;

    }


}
