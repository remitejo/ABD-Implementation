package com.lightbend.akka.sample;
import java.io.*;
import java.util.*;
import akka.actor.UntypedAbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import akka.event.Logging;
import akka.event.LoggingAdapter;


import java.util.ArrayList;


//#greeter-messages
public class Process extends UntypedAbstractActor {

  //private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);  
	private LoggingAdapter log = null;
   
  
  
    //static public Props props() {
    //  return Props.create(Process.class, () -> {
    //			return new Process();
    //  });
    //}

 static public Props props(int id, int M) {
    return Props.create(Process.class, () -> new Process(id, M));
  }

    

 
 //#system-members
  static public class Members {
    //public final int num;
    public final ArrayList<ActorRef> members; 
      
    public Members(ArrayList<ActorRef> members) {
    //    this.num = members.size();
	this.members = members;
    }
  }
    
 
 
  
  //#state, 0 - faulty, 1 - active, 2 - waiting
  static public class State {
    public final int state;
    
      public State(int state) {
        this.state = state;
    }
  }

  // local value with (seqnum, process id) timestamp
  static public class StampedValue {
    public int value;
    public int seqnum;
    public int pid;
    
      public StampedValue(int value, int seqnum, int pid) {
        this.value = value;
        this.seqnum = seqnum;
        this.pid = pid;
    }
  }

  // the "write-request" message type with a timestamped value and the local seq number
  static public class WriteRequest{
	   public StampedValue val;
	   public String key;
	   public int localseqnum;
	   
	      public WriteRequest(StampedValue val,String key, int localseqnum) {
	        this.val = val;
	        this.key = key;
	        this.localseqnum = localseqnum;
	    }
	  }
  
  // the "write-response" message type with a local seq number
  static public class WriteResponse{
	   public int localseqnum;
	   
	      public WriteResponse(int localseqnum) {
	        this.localseqnum = localseqnum;
	    }
	  }
 
  
  // the "read-request" message type with the local seq number
  static public class ReadRequest{
	    public int localseqnum;
	    public String key;
	    
	      public ReadRequest(String key, int localseqnum) {
	    	this.key = key;
	        this.localseqnum = localseqnum;
	    }
	  }

  // the "read-response" message type with a timestamped value and the local seq number
  static public class ReadResponse{
	   public StampedValue val;
	   public String key;
	   public int localseqnum;
	   
	      public ReadResponse(StampedValue val, String key, int localseqnum) {
	        this.val = val;
	        this.key = key;
	        this.localseqnum = localseqnum;
	    }
  }
  
 
  // process identifier
  private final int id;
  
  // system members known to the process
  private  Members mem;
  
  // process state
  private int state;
  
  // currently executed operation: 0 - no operation is active, 1 - get, 2- put 
  private int activeop; 
  
  // local copy of the register value
  private int newValue;
  private String key;
  private HashMap<String,StampedValue> dict = new HashMap<String,StampedValue>();
  
  // local sequence number: the number of operations performed so far
  private int localseqnum;
  
  //Nb of each put and get to do
  private int M;
  
  // local message buffer (to store messages received in a write or read phase)
  private ArrayList msgsWriter = new ArrayList();
  private ArrayList msgsReader = new ArrayList();
  
  
 
 public Process(int id, int M) { 
	this.state = 0;
	this.M = M;
	dict.put("1", new StampedValue(0,0,0));
    this.id = id;
    this.activeop = 0;
    this.localseqnum = 0;
    System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] [%4$-7s] %5$s %n");
    log = Logging.getLogger(getContext().getSystem(), this);

 }
    
 public void put(String key, int value) {
	// start a put operation 
	 this.localseqnum++;
	 //log.info("P"+this.id+": invokes put operation "+this.localseqnum+ " replacing " + this.dict.get(key).value+","+this.dict.get(key).seqnum + " by " + value + " " + this.dict.get(key).seqnum);
	 log.info("P"+this.id+": invokes put operation "+this.localseqnum+ " replacing " + this.dict.get(key).value+" by " + value);
	 this.activeop = 2; // active operation is put
	 this.msgsWriter.clear();
	 this.msgsReader.clear();
	 this.newValue = value;
	 this.key = key;
	 for(int x = 0; x < this.mem.members.size(); x = x + 1) { // get the current value/timestamp
		 this.mem.members.get(x).tell(new ReadRequest(key, this.localseqnum),getSelf()); 
	 }
	 this.state=3; // Enter the waiting state
 }
 
 public void get(String key){
	// start a put operation 
	 this.localseqnum++;
	 log.info("P"+this.id+": invokes get operation "+this.localseqnum);
	 this.activeop = 1; // active operation is get
	 this.msgsWriter.clear();
	 this.msgsReader.clear();
	 for(int x = 0; x < this.mem.members.size(); x = x + 1) { // get the current value/timestamp
		 this.mem.members.get(x).tell(new ReadRequest(key, this.localseqnum),getSelf()); 
	 }
	 this.state=3; // Enter the waiting state
 }

	@Override
	public void onReceive(Object msg) throws Exception {
		if (this.state != 2) { // If not faulty
			ActorRef actorRef = getSender();
	    if (msg instanceof Members) {
		    this.mem = (Members) msg; // determine the system members
		    //for(int x = 0; x < this.mem.members.size(); x = x + 1) {
			 //log.info(this.name+": know member "+Integer.toString(x)); 
			 //this.mem.members.get(x).tell("Message to "+this.mem.members.get(x),getSelf()); 
		     //}
	    } //else
	     //if (msg instanceof String) {
	     // 	 log.info("P"+this.id+": received a string message '"+msg+"' from "+actorRef); 	     
	     // }
	     else
	     if (msg instanceof State) { // failure indication 
	    	 	 this.state = ((State) msg).state; 
		    	 if (this.state==1) { // the process is active
		    		 this.put("1", this.id);
		    	 }
		    	 else { // the process is faulty
 		    		 log.info("P"+this.id+" is faulty"); 
		    	 }
	           }		
		  else 
		  if (msg instanceof ReadRequest) { 
			  if(this.state != 2) {
		 	    	//log.info("P"+this.id+": received a read request "+((ReadRequest)msg).localseqnum+" from "+actorRef+"with "+this.dict.get(((ReadRequest)msg).key).seqnum + " and returned seqnum : " + this.dict.get(((ReadRequest)msg).key).seqnum); 
				  //log.info(this.id + " has " + this.dict.get("1").seqnum);
				  if(!this.dict.containsKey(((ReadRequest)msg).key))
					  this.dict.put(((ReadRequest)msg).key, new StampedValue(0,0,this.id));
				  actorRef.tell(new ReadResponse(this.dict.get(((ReadRequest)msg).key),((ReadRequest)msg).key,((ReadRequest)msg).localseqnum),getSelf());
			  }
		  } 
		  else 
		  if (msg instanceof WriteRequest) {
			  				//log.info("J'ai eu: " + ((WriteRequest)msg).val.value + " " + ((WriteRequest)msg).val.seqnum + " " + ((WriteRequest)msg).val.pid);
				 	    	if (this.state != 2 && (((WriteRequest)msg).val.seqnum>this.dict.get(((WriteRequest)msg).key).seqnum || 
				 	    			((WriteRequest)msg).val.seqnum==this.dict.get(((WriteRequest)msg).key).seqnum &&
		 	    					((WriteRequest)msg).val.pid>this.dict.get(((WriteRequest)msg).key).pid)) {   
		 	    						this.dict.put(((WriteRequest)msg).key,((WriteRequest)msg).val);
				 	    				log.info("P"+this.id+": updated the local value with value ("+((WriteRequest)msg).val.value+","+((WriteRequest)msg).val.seqnum+","+((WriteRequest)msg).val.pid+")");
		 	    					}
				 	    	actorRef.tell(new WriteResponse(((WriteRequest)msg).localseqnum),getSelf());
	     }
	     else
		 if (msg instanceof ReadResponse && ((ReadResponse)msg).localseqnum==this.localseqnum  
			 	     && this.state==3) {
			 	    	// the expected response is received
				 	    	//log.info("P"+this.id+": received a read response "+this.localseqnum+" from "+actorRef);
				 	    	msgsReader.add(msg);
				 	    	// If "enough responses received:  change the state and invoke a new request
				 	    	if (msgsReader.size()>=this.mem.members.size()/2+1) {
				 	    		//log.info("P"+this.id+": received a quorum of read responses "+this.localseqnum);
				 	    		this.state=1;

				 	    		//PUT
				 	    		if (this.activeop==2) {
				 	    			StampedValue v = new StampedValue(0,0,0);
			 	    				int seqnum = this.dict.get(((ReadResponse)msg).key).seqnum;
				 	    			for (int x = 0; x<msgsReader.size(); x = x+1) {
				 	    				//log.info(this.id + " received " + ((ReadResponse)msgsReader.get(x)).val.seqnum);
					 	    			if (((ReadResponse)msgsReader.get(x)).val.seqnum>seqnum ||
					 	    					((ReadResponse)msgsReader.get(x)).val.seqnum==seqnum &&
					 	    					((ReadResponse)msgsReader.get(x)).val.pid>this.dict.get(((ReadResponse)msg).key).pid) {
					 	    				v = ((ReadResponse)msgsReader.get(x)).val;

					 	    			}
					 	    		}
					 	    		//log.info("P"+this.id+": new timestamp = ("+seqnum+","+this.dict.get(((ReadResponse)msg).key).pid+")");
					 	    		
				 	    			this.state=3; //the waiting state
				 	    			this.msgsReader.clear();
				 	    			for(int x = 0; x < this.mem.members.size(); x = x + 1) {
						    			 // write a new value (process identifier) with the incremented seq number
				 	    				//log.info(this.id + " asks " + this.mem.members.get(x) + " to change into (" + this.id+","+(seqnum+1)+","+this.id+")");
						    			this.mem.members.get(x).tell(new WriteRequest(new StampedValue(newValue,seqnum+1,this.id),((ReadResponse)msg).key,this.localseqnum),getSelf()); 
				 	    			}
				 	    		}
				 	    		//GET
			 	    			else if(this.activeop == 1) {
			 	    				for (int x = 0; x<msgsReader.size(); x = x+1) { 
			 	    					if (((ReadResponse)msgsReader.get(x)).val.seqnum>this.dict.get(((ReadResponse)msg).key).seqnum ||
				 	    					((ReadResponse)msgsReader.get(x)).val.seqnum==this.dict.get(((ReadResponse)msg).key).seqnum &&
				 	    					((ReadResponse)msgsReader.get(x)).val.pid>this.dict.get(((ReadResponse)msg).key).pid) {
			 	    						this.dict.put(((ReadResponse)msgsReader.get(x)).key,((ReadResponse)msg).val);
				 	    				
			 	    					}
			 	    				}
			 	    				//log.info("P"+this.id+": new timestamp = ("+this.dict.get(((ReadResponse)msg).key).seqnum+","+this.dict.get(((ReadResponse)msg).key).pid+")");
				 	    		
			 	    				this.state=3; //the waiting state
				 	    			this.msgsReader.clear();
		 	    					StampedValue tmp = this.dict.get(((ReadResponse)msg).key);
		 	    					//log.info(this.id + " GET "  + tmp.value);
				 	    			for(int x = 0; x < this.mem.members.size(); x = x + 1) {
						    			 // Ask everyone to update with the most recent value
						    			this.mem.members.get(x).tell(new WriteRequest(this.dict.get(((ReadResponse)msg).key), ((ReadResponse)msg).key, this.localseqnum),getSelf()); 
				 	    		
				 	    			}
				 	    		}
				 	    	}
			} 
		    else
			if (msg instanceof WriteResponse && ((WriteResponse)msg).localseqnum==this.localseqnum  
				 	     && this.state==3) {
				 	    	// the expected write response is received
					 	    	//log.info("P"+this.id+": received a write response "+this.localseqnum+" from "+actorRef);
								msgsWriter.add(msg);
					 	    	// If "enough responses received:  change the state and invoke a new request
					 	    	//log.info("Msg nb: " + msgs.size()+ "  Msg attendus : " + (this.mem.members.size()/2+1));
					 	    	if (msgsWriter.size()>=this.mem.members.size()/2+1) {
					 	    		//log.info("P"+this.id+": received a quorum of write responses "+this.localseqnum);
					 	    		this.state=1;	
					 	    		
					 	    		log.info("\t\tP"+this.id+": completes "+ ((this.activeop == 1)? "GET": "PUT") + " operation "+this.localseqnum+ ", value = "+ this.dict.get(this.key).value);

					 	    		// Finished a put operation
					 	    		//Run the next one: Random between put and get
					 	    		if(this.localseqnum < 2*this.M) {
					 	    			this.msgsWriter.clear();
					 	    			this.msgsReader.clear();
					 	    			if(this.localseqnum % 2 == 0) {
					 	    				this.put("1", this.id*(this.localseqnum/2+1));
					 	    			}
					 	    			else {
					 	    				this.get("1");
					 	    			}
					 	    		}
					 	    		else {
					 	    		    String diff = Main.seconds.format((new Date()).getTime() - Main.Birth.getTime());
					 	    		    log.info("Process " + this.id + " execution time: " + diff + " second(s).");
					 	    		}
					 	    	}
					 	    	
				 	}
		 	else
			  unhandled(msg);
	}
	}

  
  
}

