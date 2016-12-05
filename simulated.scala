
// IMPLEMENTING SIMULATED ANNEALING FOR TRAVELLING SALESMAN PROBLEM IN SPARK USING SCALA AS PROGRAMMING LANGUAGE 

//Based on the tutorial in www.projectcode.com-by Lee Jacob 

package SparkApplication  

import org.apache.spark.{SparkConf, SparkContext} // importing spark API 

import scala.annotation.tailrec  


object SparkMain {
  val (initialTemperature, coolingRate) = (1000000.0, 0.0004) //providing with initial temperature and cooling rate 

  case class City(x:Int,y:Int) // constructing a city at random x and y position on the graph
                                // (external dataset is provided as text file) 

//to get the distance between two of the given city 
  def distance(a:City, b:City) = math.sqrt(List(a.x - b.x, a.y - b.y).map(x=>x*x).sum)

//randomly reorder the tour  
  def mkTour(tour:Seq[City]) = {
    val (i,j) = ( util.Random.nextInt(tour.size), util.Random.nextInt(tour.size) )
    tour
      .patch(i,Seq(tour(j)),1)
      .patch(j,Seq(tour(i)),1)
  }

  def length(tour:Seq[City]) = tour.foldLeft(0.0,tour.head)((a,b)=>(a._1 + distance(a._2,b),b))._1

  // If the new solution is better, accept it, else compute acceptance probability
  def acceptanceProbability(energies:(Double,Double), temperature:Double) = {
    val diff = energies._1 - energies._2
    if (diff < 0 ) 1.0 else math.exp(diff/temperature)
  }
 


  @tailrec
  def compute( best:Seq[City], temp:Double):Seq[City] = {
    //compute till temperature is >1
    if (temp > 1) {
    
      val newSolution = mkTour( best ) //calling function mk 
      val currentsolution = length(best)
      val neighbourSolution = length(newSolution)

      // Decide if we should accept the neighbour
      val accept = (acceptanceProbability((currentsolution, neighbourSolution), temp) > math.random) &&
        (length(newSolution) < length(best))
      compute( if (accept) {
        printf("\nLength: %.2f, Temperature: %.2f", length(newSolution), temp)
        newSolution
      } else best, (1-coolingRate)*temp) //decreasing the cooling rate 
    } else best
  }


// main function 
  def main(args:Array[String]) = {
    if(args.length < 1) throw new Exception("File Path to Dataset is required")
    val conf = new SparkConf().setAppName("Spark application").setMaster("local[*]")  //setting the spark configuration ,application namr and the executor 
    val sc = new SparkContext(conf)  //creating new spark context
    val tour = sc.textFile("file://"+args(0))  //fetching the dataset 
    val data = tour.flatMap(_.split("\n")).map(line => City(line.split(",")(0).toInt,line.split(",")(1).toInt)) //reading the data from the file and using .map to map the city's
    val best = compute(data.collect().toSeq, initialTemperature) //calling the compute method 
    
//output the final solution 
    printf("\nFinal solution distance: %.2f\n",length(best))
  // print the best tour path 
    println("Tour: " + best)
  }
}

