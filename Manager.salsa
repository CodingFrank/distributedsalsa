module assignment2;

import java.io.*;
import java.util.*;

/* Manager.salsa -- Main program to manage/distribute workers/rankers.
 * 
 *
 */

behavior Manager 
{
	int numberOfRankers;	    // number of workers
	String inputFile;			// data file
	
	Double[][] cell;
	
	long startTime; 			// starting time
	
	String theaterConfiguration;		// File containing the theaters
	String nameServer;			// Name Server	
	boolean distributedTrue = false;
	
	/*
	 * Asking for arguments and read the input file
	 * 
	 * Arguments:
	 * 1) Number of rankers (required)
	 * Sample: 5
	 * 2) Input file (required)
	 * Sample: stars_9_xyz.txt
	 * 3) Theater Configuration File (Where each line stands for a theator)
	 * A file to contain UALs of theaters
	 * Sample data in this file 
	 * 129.161.134.20:4040
	 * 129.161.134.20:4041
	 * 129.161.134.20:4042
	 * Sample: --end----
	 * 4) Nameserver
	 * Sample: 129.161.134.20:3030
	 */
	void act(String args[]) 
	{
        if (args.length < 2) 
        {
            standardOutput<-println( "Usage: java -cp yoursalsajarfile;. assignment2.Manager <numberOfRankers> " +
            		"<inputFile> <theaterConfiguration> <nameServer>\nnumberofRankers, inputFile are " +
            		"mandatory, \n and both the theaterConfigurationFile and nameServer are required if you are running in a " +
            		"distributed mode.");
            return;
        }
        else
        {
        	numberOfRankers = Integer.valueOf(args[0]).intValue();
        	inputFile = args[1];
        }
		if (args.length >= 3)
			theaterConfiguration = args[2];
		if (args.length >= 4)
		{
			distributedTrue = true;
			nameServer = args[3];
		}
		
		startTime = System.currentTimeMillis();
		
		readData() @ distributeWorkers(token) @ outputResult(token);
	}

	/**
	 * Reads data from specified data file.  Returns the number of stars read.
	 */
	int readData() 
	{
		int countOfStars = 0;
		
		try 
		{
			BufferedReader in = new BufferedReader(new FileReader(inputFile));
			// read the first line to get the number of stars
			String line = in.readLine();
			countOfStars = Integer.valueOf(line).intValue();
			
			// allocate an matrix of stars 
			cell = new Double[countOfStars][3];
			int i = 0;
			
			for ( i = 0; i < countOfStars; i++ )
			{
				if ( (line = in.readLine()) == null )
					standardOutput<-println("Error: number of stars does not match number of rows in " + inputFile);
				else
				{
					// read line into data matrix
					StringTokenizer strTok = new StringTokenizer(line);
					for ( int j = 0; j < 3; j++ )
						cell[i][j] = Double.valueOf(strTok.nextToken());
				}
			}
			
			if ( i != countOfStars ) 
				standardOutput<-println("Mismatch between number of stars and number of rows in " + inputFile);
		} 
		catch (IOException ioEx)
		{
			standardOutput<-println("Iput file cannot be openned, please first verify the input file " + inputFile + ": " + ioEx.getMessage());
		}
		
		return countOfStars;
	}
		
	String distributeWorkers(int countOfStars) 
	{
		Worker[] workers = new Worker[numberOfRankers];
		
		if ( distributedTrue )
		{
			// read theaters
		    Vector theaters = new Vector();
		    String theater;
		    try {
		      BufferedReader in = new BufferedReader(new FileReader(theaterConfiguration));
		      while ((theater = in.readLine())!= null){
		        theaters.add(theater);
		      }
		      in.close();
		    } catch (IOException ioe){
		      standardOutput<-println("Error: Can't open the file "+theaterConfiguration+" for reading.");
		    }
		    
			// allocate workers through the theaters
			for ( int i = 0; i < numberOfRankers; i++ )
			{
				standardOutput <- println("Sending worker "+ i + 
						" with Name Server uan://"+nameServer+"/assignment2_"+i+" to theater "+
			  	        "rmsp://"+theaters.get(i%theaters.size())+"/assignment2_"+i);
			
				workers[i] = new Worker() at
		        	( new UAN("uan://"+nameServer+"/a"+i),
	    			  new UAL("rmsp://"+theaters.get(i%theaters.size())+"/a"+i) );
		    }
		}
		else
		{
			for ( int i = 0; i < numberOfRankers; i++ )
				workers[i] = new Worker();
		}
		
		standardOutput <- println(numberOfRankers + " actors are generated to do the computing of " + countOfStars + " stars.");

		join 
		{
			// calculate the number of stars for each worker to calculate
			int starNum = 0;
			
			// allocate the computing to all workers but the very last one
			for ( int i = 0; i < numberOfRankers - 1; i++ )
			{
				double starsToCalc = (countOfStars - starNum - 0.5) - 
					Math.sqrt( Math.pow( countOfStars - starNum - 0.5, 2 ) - 
							   ( Math.pow( countOfStars, 2 ) - countOfStars ) / numberOfRankers );
				int starsPerWorker = (int)Math.floor(starsToCalc);
				
				workers[i] <- calculateDistances(starNum,starNum+starsPerWorker-1,cell);
				starNum += starsPerWorker;
			}
			
			// the last worker will always computing the remaining part
			int starsPerLastWorker = countOfStars - starNum;
			workers[numberOfRankers-1] <- calculateDistances(starNum,starNum+starsPerLastWorker-1,cell);
		} @ caculateResults(token) @ currentContinuation;		
	}

	String caculateResults(Object[] processedData)
	{
		/**  first compose per-star results **/
				
		// create results container for all cell points
		MatrixSaver[] results = new MatrixSaver[cell.length];
		for ( int i = 0; i < cell.length; i++ )
			results[i] = new MatrixSaver(i);
		
		// compile various results containers into one results container per star
		standardOutput <- println("Number of results arrays to process: " + processedData.length);
		for ( int i = 0; i < processedData.length; i++ )
		{
			MatrixSaver[] rcArr = (MatrixSaver[]) processedData[i];
			standardOutput <- println("Size of results array " + i + ": " + rcArr.length);
			
			for ( int j = 0; j < rcArr.length; j++ )
			{
				MatrixSaver rc = rcArr[j];
				
				results[rc.getStar()].updateMinDist(rc.getMinDist(),rc.getMinDistStars());
				results[rc.getStar()].updateMaxDist(rc.getMaxDist(),rc.getMaxDistStars());
				results[rc.getStar()].addToSumDists(rc.getSumDists());
			}
		}
		
		/** determine results **/
		
	    double minDist = Double.MAX_VALUE;
	    Vector minDistPairs = new Vector(); // Vector<Integer[]>
	    
	    double maxDist = 0.0;
	    Vector maxDistPairs = new Vector(); //  Vector<Integer[]>
	    
	    double minMaxDistance = Double.MAX_VALUE;
	    Vector hubStars = new Vector(); // Vector<Integer[]>
	    
	    double maxMinDistance = 0.0;
	    Vector jailStars = new Vector(); // Vector<Integer[]>
	    
	    double minAvgDistance = Double.MAX_VALUE;
	    Vector capStars = new Vector(); // Vector<Integer>
	    
	    for ( int i = 0; i < cell.length; i++ )
	    {
	    	MatrixSaver rc = results[i];
	    	
			// check if minimal-distant pair
			if ( rc.getMinDist() < minDist )
				minDistPairs.clear(); // unique minimum-distant pair
			if ( rc.getMinDist() <= minDist )
			{
				for ( Iterator iter = rc.getMinDistStars().iterator(); iter.hasNext(); )
				{
					Integer[] pair = new Integer[2];
					pair[0] = rc.getStar();
					pair[1] = (Integer)iter.next();
					
					// add to vector if doesn't already exist
					boolean exists = false;
					for ( Iterator eIter = minDistPairs.iterator(); eIter.hasNext() && !exists; )
					{
						Integer[] vInt = (Integer[])eIter.next();
						if ( (pair[0].equals(vInt[0]) && pair[1].equals(vInt[1]) ) ||
						 	 (pair[1].equals(vInt[0]) && pair[0].equals(vInt[1]) ) )
							exists = true;
					}
					if (!exists)
						minDistPairs.add(pair);
				}
				minDist = rc.getMinDist();
			}
			
			// check if maximal-distant pair
			if ( rc.getMaxDist() > maxDist )
				maxDistPairs.clear(); // unique maximum-distant pair
			if ( rc.getMaxDist() >= maxDist )
			{
				for ( Iterator iter = rc.getMaxDistStars().iterator(); iter.hasNext(); )
				{
					Integer[] pair = new Integer[2];
					pair[0] = rc.getStar();
					pair[1] = (Integer)iter.next();

					// add to vector if doesn't already exist
					boolean exists = false;
					for ( Iterator eIter = maxDistPairs.iterator(); eIter.hasNext() && !exists; )
					{
						Integer[] vInt = (Integer[])eIter.next();
						if ( (pair[0].equals(vInt[0]) && pair[1].equals(vInt[1]) ) ||
						 	 (pair[1].equals(vInt[0]) && pair[0].equals(vInt[1]) ) )
							exists = true;
					}
					if (!exists)
						maxDistPairs.add(pair);
				}
				maxDist = rc.getMaxDist();
			}
			
			// check if new minimal max-distance star (hub star)
			if ( rc.getMaxDist() < minMaxDistance )
				hubStars.clear(); // unique min-max-distance star
			if ( rc.getMaxDist() <= minMaxDistance )
			{
				minMaxDistance = rc.getMaxDist();
				
				for ( Iterator iter = rc.getMaxDistStars().iterator(); iter.hasNext(); )
				{
					Integer otherStar = (Integer)iter.next();
					
					Integer[] pair = new Integer[2];
					pair[0] = rc.getStar();
					pair[1] = otherStar;
					
					hubStars.add(pair);
				}
			}
			
			// check if new maximal min-distance star (jail star)
			if ( rc.getMinDist() > maxMinDistance )
				jailStars.clear(); // unique max-min-distance star
			if ( rc.getMinDist() >= maxMinDistance )
			{
				maxMinDistance = rc.getMinDist();
				
				// add to vector if not exist
				for ( Iterator iter = rc.getMinDistStars().iterator(); iter.hasNext(); )
				{
					Integer otherStar = (Integer)iter.next();
					
					Integer[] pair = new Integer[2];
					pair[0] = rc.getStar();
					pair[1] = otherStar;
					
					jailStars.add(pair);
				}
			}
			
			// check if new minimal average distance star (capital star)
			double avgDist = rc.getSumDists()/(cell.length-1); 
			if ( avgDist < minAvgDistance ) 
				capStars.clear(); // unique minimum average distance star
			if ( avgDist <= minAvgDistance )
			{
				minAvgDistance = avgDist;
				
				// add to vector if doesn't already exist
				capStars.add(rc.getStar());	
			}	
	    }
	    
	    StringBuilder sb = new StringBuilder();
	    sb.append(minDist + " // minimal pairwise distance\n");
	    for ( Iterator iter = minDistPairs.iterator(); iter.hasNext(); )
	    {
	    	Integer[] iArr = (Integer[]) iter.next();
	    	sb.append(getStarString(cell[iArr[0]]) + "\n" + iArr[1] + " " + getStarString(cell[iArr[1]]) + "\n");
	    }
	    sb.append("\n");
	    sb.append(maxDist + " // maximal pairwise distance\n");
	    for ( Iterator iter = maxDistPairs.iterator(); iter.hasNext(); )
	    {
	    	Integer[] iArr = (Integer[]) iter.next();
	    	sb.append(getStarString(cell[iArr[0]]) + "\n" + getStarString(cell[iArr[1]]) + "\n");
	    }
	    sb.append("\n");
	    sb.append(minMaxDistance + " // minimum maximal distance\n");
	    for ( Iterator iter = hubStars.iterator(); iter.hasNext(); )
	    {
	    	Integer[] stars = (Integer[])iter.next();
	    	sb.append(getStarString(cell[stars[0]]) + "\n" + getStarString(cell[stars[1]]) + "\n");
	    }
	    sb.append("\n");
	    sb.append(maxMinDistance + " // maximum minimal distance\n");
	    for (  Iterator iter = jailStars.iterator(); iter.hasNext(); )
	    {
	    	Integer[] stars = (Integer[])iter.next();
	    	sb.append(getStarString(cell[stars[0]]) + "\n" + getStarString(cell[stars[1]]) + "\n");
	    }
	    sb.append("\n");
	    sb.append(minAvgDistance + " // minimal average distance\n");
	    for ( Iterator iter = capStars.iterator(); iter.hasNext(); )
	    {
	    	Integer star = (Integer)iter.next();
	    	sb.append(getStarString(cell[star]) + "\n");
	    }
	    sb.append("\n");
	    
	    return sb.toString();
	}	    
	
	void outputResult(String resultStr)
	{
	    long endTime = System.currentTimeMillis();
	    long runTime = (endTime - startTime)/1000;
	    standardOutput <- println("The total running time for the whole process is: " + runTime + "seconds.") @ 	    
	    standardOutput <- println(resultStr);    	
	}
	
	private String getStarString(Double[] starXYZ)
	{
		return starXYZ[0] + " " + starXYZ[1] + " " + starXYZ[2];
	}
}