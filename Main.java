package com.lightbend.akka.sample;

import java.io.IOException;

import com.lightbend.akka.sample.Process.Members;
import com.lightbend.akka.sample.Process.State;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

//To print time 
import java.text.SimpleDateFormat;
import java.util.Date;
//

import java.util.ArrayList;

import java.util.Collections;

public class Main {
	
	public static Date Birth; 
	public static SimpleDateFormat seconds = new SimpleDateFormat("s.SSS");
	
    public static void main(String[] args) {
	
	final int N = 3; // The system size 
	final int M = 3; //Nb of get to perform
	
	final ActorSystem system = ActorSystem.create("system");

    final ArrayList<ActorRef> members = new ArrayList<ActorRef>();
    Main.Birth = new Date();
    
    try {
      
    	
    	//#create-actors
    	for(int x = 0; x <= N-1; x = x + 1) {
	     members.add(system.actorOf(Process.props(x,M), "P"+Integer.toString(x)));
	}    


       SimpleDateFormat dateFormatter = new SimpleDateFormat("E m/d/y h:m:s.SSS z");
       System.out.println("System birth: "+ dateFormatter.format(Birth));

       for(int x = 0; x < N; x = x + 1) {
	   members.get(x).tell(new Members(members), ActorRef.noSender());      
	}

    // shuffle and choose 1/3 random processes to fail
       
    Collections.shuffle(members);
    
    for(int x = 0; x < N/2+1; x = x + 1) { // first 2/3 processes are active
    	   members.get(x).tell(new State(1), ActorRef.noSender());      
    } 
    for(int x = N/2+1; x < N; x = x + 1) { // last 1/3 processes are faulty 
     	   members.get(x).tell(new State(2), ActorRef.noSender());
     	   //System.out.println("Process "+ members.get(x) +" is faulty");
     }
    
	
      //#main-send-messages

      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
    } catch (IOException ioe) {
    } finally {
      system.terminate();
    }
  }
}
