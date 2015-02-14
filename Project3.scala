import akka.actor._
import java.util.concurrent.TimeUnit
import scala.Char._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.math._
import scala.util.Random

sealed trait Messages
case class AddLeaf(leafSet: ArrayBuffer[Int]) extends Messages
case class AddRow(rowNum: Int, row: Array[Int]) extends Messages
case class InitialJoin(initial : ArrayBuffer[Int]) extends Messages
case class Route(msg: String, requestFrom: Int, requestTo: Int, hops: Int) extends Messages
case class RouteFinish(requestFrom: Int, requestTo: Int, hops: Int) extends Messages
case class Update(newNodeID: Int) extends Messages
case object Acknowledgement extends Messages
case object DisplayLeafAndRouting extends Messages
case object FinishedJoining extends Messages
case object NotInBoth extends Messages
case object RouteNotInBoth extends Messages
case object SecondaryJoin extends Messages
case object Start extends Messages 
case object StartRouting extends Messages 



object Project3 extends App{
	/*
		*	numNodes are the total no of actor that will participate in the network 
		*	numRequest are total no of request made by each actor	
		*
	*/
	var numNodes = 0
	var numRequests = 0
	if(args.length != 2){
		println("************************Invalid Input**********************\n" + 
				"Input should be of type : \n scala Project3 numNodes numRequests \n" +
				"***********************************************************")
		System.exit(1)
	}
	else{
			numNodes = args(0).toInt
			numRequests = args(1).toInt
			println("Num Nodes : " + numNodes + "Num Requests : " + numRequests);
			pastry_protocol( numNodes , numRequests)
	}
	def pastry_protocol(numNodes : Int , numRequests : Int){
		val system = ActorSystem("Project3")
      	val master = system.actorOf(Props(new Master(numNodes, numRequests)), name = "master")
      	master ! Start
	}
}

class Master (numNodes : Int , numRequests : Int) extends Actor{
	/*
		*  Master Class that will  construct all the actors and  help them to join the network
			*	Base variable used calculate the max possible rows in Routing Table with base b=4
			*	randomList is an arraybuffer used to evaluate the random id of the all
			*		
	*/
	var base = ceil(log(numNodes.toDouble) / log(4)).toInt
	var nodeIDSpace: Int = pow(4, base).toInt
	var randomList = new ArrayBuffer[Int]()
  	var groupOne = new ArrayBuffer[Int]()
  	var groupOneSize: Int = if (numNodes <= 1024) numNodes else 1024
  	var i = -1
  	var numHops = 0
  	var numJoined = 0
  	var numNotInBoth = 0
  	var numRouteNotInBoth = 0
  	var numRouted = 0
  	
  	for (i <- 0 until nodeIDSpace) { 		
    	randomList += i
  	}
  	randomList = Random.shuffle(randomList) 

  	for (i <- 0 until groupOneSize) {
   	 groupOne += randomList(i)
  	}

  	for (i <- 0 until numNodes) {
   	 context.actorOf(Props(new PastryNode(numNodes, numRequests, randomList(i), base)), name = String.valueOf(randomList(i))) 
  	}
	def receive = {
		case Start => 
			println("Joining")
      		for (i <- 0 until groupOneSize)
        		context.system.actorSelection("/user/master/" + randomList(i)) ! InitialJoin(groupOne.clone)

	        case FinishedJoining =>
			      numJoined += 1
			      if (numJoined == groupOneSize) {
			        //println("Initial Join Finished!")
			        if (numJoined >= numNodes) {
			          self ! StartRouting
			        } else {
			          self ! SecondaryJoin
			        }
			      }

			      if (numJoined > groupOneSize) {
			        if (numJoined == numNodes) {
			          //println("Routing Not In Both Count: " + numNotInBoth)
			          //println("Ratio: " + (100 * numNotInBoth.toDouble / numNodes.toDouble) + "%")
			          self ! StartRouting
			        } else {
			          self ! SecondaryJoin
			        }

			      }

	    case SecondaryJoin =>
	      val startID = randomList(Random.nextInt(numJoined))
	      context.system.actorSelection("/user/master/" + startID) ! Route("Join", startID, randomList(numJoined), -1)

	    case StartRouting =>
		  println("Joined")
	      println("Routing")
	      context.system.actorSelection("/user/master/*") ! StartRouting

	    case NotInBoth =>
	      numNotInBoth += 1

	    case RouteFinish(requestFrom, requestTo, hops) =>
	      numRouted += 1
	      numHops += hops
	      for (i <- 1 to 10)
	        if (numRouted == numNodes * numRequests * i / 10){
	          for ( j <- 1 to i)
	          	print(".")
	          print("|")
	        }

	      if (numRouted >= numNodes * numRequests) {
	      	println("\n")
	        println("Total Routes -> " + numRouted + " Total Hops -> " + numHops)
	        println("Average Hops Per Route -> " + numHops.toDouble / numRouted.toDouble)
	        context.system.shutdown()
	      }

	    case RouteNotInBoth =>
	      numRouteNotInBoth += 1
	}
}

class PastryNode(numNodes : Int , numRequests : Int , myID : Int , base : Int) extends Actor{ 
	import context._
	var smallerLeaf = new ArrayBuffer[Int]()
  	val IDSpace: Int = pow(4, base).toInt
  	var largerLeaf = new ArrayBuffer[Int]() 
  	var numOfBack: Int = 0
  	var routingTable = new Array[Array[Int]](base) 

  	var i = 0
  	for (i <- 0 until base)
    	routingTable(i) = Array(-1, -1, -1, -1)

	    def toBase4String(raw: Int, length: Int): String = {
	    var str: String = Integer.toString(raw, 4)
	    val diff: Int = length - str.length()
	    if (diff > 0) {
	      var j = 0
	      while (j < diff) {
	        str = '0' + str
	        j += 1
	      }
	    }
	    return str
	  }

	  def checkPrefix(string1: String, string2: String): Int = {
	    var j = 0
	    while (j < string1.length && string1.charAt(j).equals(string2.charAt(j))) {
	      j += 1
	    }
	    return j
	  }

	  def addBuffer(all: ArrayBuffer[Int]): Unit = {
	    for (i <- all) {
	    	//Can be added to larger side
	      if (i > myID && !largerLeaf.contains(i)) { 
	        if (largerLeaf.length < 4) {
	          largerLeaf += i
	        } else {
	          if (i < largerLeaf.max) {
	            largerLeaf -= largerLeaf.max
	            largerLeaf += i
	          }
	        }
	      }	// Can be added on smaller side 
	      else if (i < myID && !smallerLeaf.contains(i)) { 
	        if (smallerLeaf.length < 4) {
	          smallerLeaf += i
	        } else {
	          if (i > smallerLeaf.min) {
	            smallerLeaf -= smallerLeaf.min
	            smallerLeaf += i
	          }
	        }
	      }
	      //Check the routing routingTable
	      var samePrefix = checkPrefix(toBase4String(myID, base), toBase4String(i, base))
	      if (routingTable(samePrefix)(toBase4String(i, base).charAt(samePrefix).toString.toInt) == -1) {
	        routingTable(samePrefix)(toBase4String(i, base).charAt(samePrefix).toString.toInt) = i
	      }
	    }
	  }

	  def addNode(node: Int): Unit = {
	  	//Can be added to larger side
	    if (node > myID && !largerLeaf.contains(node)) {
	      if (largerLeaf.length < 4) {
	        largerLeaf += node
	      } else {
	        if (node < largerLeaf.max) {
	          largerLeaf -= largerLeaf.max
	          largerLeaf += node
	        }
	      }
	    } //Can be added to smaller side 
	    else if (node < myID && !smallerLeaf.contains(node)) { 
	      if (smallerLeaf.length < 4) {
	        smallerLeaf += node
	      } else {
	        if (node > smallerLeaf.min) {
	          smallerLeaf -= smallerLeaf.min
	          smallerLeaf += node
	        }
	      }
	    }
	    //check routing routingTable
	    var samePrefix = checkPrefix(toBase4String(myID, base), toBase4String(node, base))
	    if (routingTable(samePrefix)(toBase4String(node, base).charAt(samePrefix).toString.toInt) == -1) {
	      routingTable(samePrefix)(toBase4String(node, base).charAt(samePrefix).toString.toInt) = node
	    }
	  }

	  def display(): Unit = {
	    println("smallerLeaf:" + smallerLeaf)
	    println("largerLeaf:" + largerLeaf)
	    for (i <- 0 until base) {
	      var j = 0
	      print("Row " + i + ": \n")
	      for (j <- 0 until 4)
	        print(routingTable(i)(j) + " ")
	      print("\n")
	    }
	  }

    def receive ={

    		case StartRouting =>
											      for (i <- 1 to numRequests)
											        context.system.scheduler.scheduleOnce(1000 milliseconds, self, Route("Route", myID, Random.nextInt(IDSpace), -1))
											    //self ! Route("Route", myID, Random.nextInt(IDSpace), -1)

	    	case InitialJoin(groupOne) =>		
	    								 /*
									          	*	We have all the random id's or probable id since we got the message so delete my id from it and keep all other random id
									          	*	Now the make the leafset of node through addbuffer function.
									          	*	Tell master about joining the network.
									     */
									      groupOne -= myID

									      addBuffer(groupOne)

									      for (i <- 0 until base) {
									        routingTable(i)(toBase4String(myID, base).charAt(i).toString.toInt) = myID
									      }

									      sender ! FinishedJoining

			// Routing Algorithm

			case Route(msg, requestFrom, requestTo, hops) =>
									      
									      if (msg == "Join") {
									      	/*
									      		* If myid and id of helping node has more than one bit in prefix than current then take all the valid entries from its rtable into my routingtable 
									      		* eg . rt(0), rt(1),rt(2)
									      	
									      	*/
									        var samePrefix = checkPrefix(toBase4String(myID, base), toBase4String(requestTo, base))
									        if (hops == -1 && samePrefix > 0) {
									          for (i <- 0 until samePrefix) {
									            context.system.actorSelection("/user/master/" + requestTo) ! AddRow(i, routingTable(i).clone)
									          }
									        }
									        context.system.actorSelection("/user/master/" + requestTo) ! AddRow(samePrefix, routingTable(samePrefix).clone)
									        /*
										         * If id of helping node leafset can be used for next routing or not. 
										         * Checking all entries in the smaller leaf table and routing the message to node with smallest differnce (proximity). 
										         * Whether helping node id in range or not
									        */
									        if ((smallerLeaf.length > 0 && requestTo >= smallerLeaf.min && requestTo <= myID) || //In less leaf set
									          (largerLeaf.length > 0 && requestTo <= largerLeaf.max && requestTo >= myID)) { //In larger leaf set
									          var diff = IDSpace + 10
									          var nearest = -1
									          if (requestTo < myID) { //In smaller leaf set
									            for (i <- smallerLeaf) {
									              if (abs(requestTo - i) < diff) {
									                nearest = i
									                diff = abs(requestTo - i)
									              }
									            }
									          } else { //In larger leaf set
									            for (i <- largerLeaf) {
									              if (abs(requestTo - i) < diff) {
									                nearest = i
									                diff = abs(requestTo - i)
									              }
									            }
									          }

									          if (abs(requestTo - myID) > diff) { // In leaf but not near my id
									            context.system.actorSelection("/user/master/" + nearest) ! Route(msg, requestFrom, requestTo, hops + 1)
									          } 
									          else { 	/*
									          				*Converging condition this is closest id possible
									          				*Take the neighbor hood set of this .
									          				*Give leafset info
									          			*/
									            var allLeaf = new ArrayBuffer[Int]()
									            allLeaf += myID ++= smallerLeaf ++= largerLeaf
									            context.system.actorSelection("/user/master/" + requestTo) ! AddLeaf(allLeaf) //Give leaf set info
									          }

									        } 
									        /*
									          	* If smaller element of the leaf is lesser than nodeid route message to it.
									          	* else if largest element of the leafset is greater than nodeid route message to it.
									          	* If no element in leafset in add leaf set info.
									          	* If no element in the leafset try routing through routing table r(l)(dl)
									          	* if not in routing table also route  then route to largestleaf if(myid< helping nodeid) else route to smalestleaf
									         */
									        else if (smallerLeaf.length < 4 && smallerLeaf.length > 0 && requestTo < smallerLeaf.min) {
									          context.system.actorSelection("/user/master/" + smallerLeaf.min) ! Route(msg, requestFrom, requestTo, hops + 1)
									        } 
									        else if (largerLeaf.length < 4 && largerLeaf.length > 0 && requestTo > largerLeaf.max) {
									          context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(msg, requestFrom, requestTo, hops + 1)
									        }
									         else if ((smallerLeaf.length == 0 && requestTo < myID) || (largerLeaf.length == 0 && requestTo > myID)) {
									          //I am the nearest
									          var allLeaf = new ArrayBuffer[Int]()
									          allLeaf += myID ++= smallerLeaf ++= largerLeaf
									          context.system.actorSelection("/user/master/" + requestTo) ! AddLeaf(allLeaf) //Give leaf set info
									        } else if (routingTable(samePrefix)(toBase4String(requestTo, base).charAt(samePrefix).toString.toInt) != -1) { 
									           //Not in leaf set, try routing routingTable
									          context.system.actorSelection("/user/master/" + routingTable(samePrefix)(toBase4String(requestTo, base).charAt(samePrefix).toString.toInt)) ! Route(msg, requestFrom, requestTo, hops + 1)
									        } else if (requestTo > myID) { 
									           //Not in both
									          context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(msg, requestFrom, requestTo, hops + 1)
									          context.parent ! NotInBoth
									        } else if (requestTo < myID) {
									          context.system.actorSelection("/user/master/" + smallerLeaf.min) ! Route(msg, requestFrom, requestTo, hops + 1)
									          context.parent ! NotInBoth
									        } else {
									          println("Not Possible")
									        }

									      } else if (msg == "Route") { //Message = Route, begin sending message
									         /*
									          	* If myid =requesnodeid stop the routing else go on .
									          	* Else find the nearest node in proximity and check its leafset and routing table till we reach its table
									         */
									        if (myID == requestTo) {
									          context.parent ! RouteFinish(requestFrom, requestTo, hops + 1)
									        } else {
									          var samePrefix = checkPrefix(toBase4String(myID, base), toBase4String(requestTo, base))

									          if ((smallerLeaf.length > 0 && requestTo >= smallerLeaf.min && requestTo < myID) || //In less leaf set
									            (largerLeaf.length > 0 && requestTo <= largerLeaf.max && requestTo > myID)) { //In larger leaf set
									            var diff = IDSpace + 10
									            var nearest = -1
									            if (requestTo < myID) { //In smaller leaf set
									              for (i <- smallerLeaf) {
									                if (abs(requestTo - i) < diff) {
									                  nearest = i
									                  diff = abs(requestTo - i)
									                }
									              }
									            } else { //In larger leaf set
									              for (i <- largerLeaf) {
									                if (abs(requestTo - i) < diff) {
									                  nearest = i
									                  diff = abs(requestTo - i)
									                }
									              }
									            }
									             // In leaf but not near my id
									            if (abs(requestTo - myID) > diff) {
									              context.system.actorSelection("/user/master/" + nearest) ! Route(msg, requestFrom, requestTo, hops + 1)
									            } else { // Nearest
									              context.parent ! RouteFinish(requestFrom, requestTo, hops + 1)
									            }

									          } else if (smallerLeaf.length < 4 && smallerLeaf.length > 0 && requestTo < smallerLeaf.min) {
									            context.system.actorSelection("/user/master/" + smallerLeaf.min) ! Route(msg, requestFrom, requestTo, hops + 1)
									          } else if (largerLeaf.length < 4 && largerLeaf.length > 0 && requestTo > largerLeaf.max) {
									            context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(msg, requestFrom, requestTo, hops + 1)
									          } else if ((smallerLeaf.length == 0 && requestTo < myID) || (largerLeaf.length == 0 && requestTo > myID)) {
									            //I am the nearest
									            context.parent ! RouteFinish(requestFrom, requestTo, hops + 1)
									          } else if (routingTable(samePrefix)(toBase4String(requestTo, base).charAt(samePrefix).toString.toInt) != -1) { //Not in leaf set, try routing routingTable
									            context.system.actorSelection("/user/master/" + routingTable(samePrefix)(toBase4String(requestTo, base).charAt(samePrefix).toString.toInt)) ! Route(msg, requestFrom, requestTo, hops + 1)
									          } else if (requestTo > myID) { //Not in both
									            context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(msg, requestFrom, requestTo, hops + 1)
									            context.parent ! RouteNotInBoth
									          } else if (requestTo < myID) {
									            context.system.actorSelection("/user/master/" + smallerLeaf.min) ! Route(msg, requestFrom, requestTo, hops + 1)
									            context.parent ! RouteNotInBoth
									          } else {
									            println("Not Possible")
									          }
									        }
									      }
				case AddRow(rowNum, newRow) =>
											    /*
									          		* Function to update the routing table through adding appropriate row.
									         	*/
											      for (i <- 0 until 4)
											        if (routingTable(rowNum)(i) == -1)
											          routingTable(rowNum)(i) = newRow(i)

				case AddLeaf(allLeaf) =>		/*
									          		* Function to update the leafset through adding appropriate entry else update the routing table if possible.
									         	*/
											      addBuffer(allLeaf)
											      //display
											      for (i <- smallerLeaf) {
											        numOfBack += 1
											        context.system.actorSelection("/user/master/" + i) ! Update(myID)
											      }
											      for (i <- largerLeaf) {
											        numOfBack += 1
											        context.system.actorSelection("/user/master/" + i) ! Update(myID)
											      }
											      for (i <- 0 until base) {
											        var j = 0
											        for (j <- 0 until 4)
											          if (routingTable(i)(j) != -1) {
											            numOfBack += 1
											            context.system.actorSelection("/user/master/" + routingTable(i)(j)) ! Update(myID)
											          }
											      }
											      for (i <- 0 until base) {
											        routingTable(i)(toBase4String(myID, base).charAt(i).toString.toInt) = myID
											      }

				case Update(newNodeID) =>
											      addNode(newNodeID)
											      sender ! Acknowledgement

				case Acknowledgement =>
											      numOfBack -= 1
											      if (numOfBack == 0)
											        context.parent ! FinishedJoining

				case DisplayLeafAndRouting =>
											      display()
											      context.system.shutdown()

				case _ =>
											      println("Wrong message")

    }

    
	
}