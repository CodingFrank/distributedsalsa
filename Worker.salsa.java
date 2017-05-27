module assignment2;

import java.io.*;
import java.util.*;

/* Worker.salsa -- worker to calcuate the distance
 */

behavior Worker 
{
	MatrixSaver[] calculateDistances(int startStar, int endStar, Double[][] data)
	{
		long startTime = System.currentTimeMillis();
		standardOutput <- println("Current thread is computing distances from star " + startStar + 
				" through " + endStar);
		
		// create results container for all data points needed for this worker
		MatrixSaver[] results = new MatrixSaver[data.length-startStar];
		for ( int i = 0; i < results.length; i++ )
			results[i] = new MatrixSaver(startStar+i);
		
		for ( int star = startStar; star <= endStar; star++ )
		{
			for ( int i = star+1; i < data.length; i++ )
			{
				// calculate distance				
				double dist = Math.sqrt(Math.pow(data[star][0] - data[i][0],2) + 
									    Math.pow(data[star][1] - data[i][1],2) + 
									    Math.pow(data[star][2] - data[i][2],2) );
				
				// update results of star
				results[star-startStar].updateMinDist(dist,i);
				results[star-startStar].updateMaxDist(dist,i);
				results[star-startStar].addToSumDists(dist);
				
				// update results of other star
				results[i-startStar].updateMinDist(dist,star);
				results[i-startStar].updateMaxDist(dist,star);
				results[i-startStar].addToSumDists(dist);
			}
		}
		
		long endTime = System.currentTimeMillis();
		standardOutput <- println("Current thread has done the computing from stars " + startStar + " through " + 
				endStar + " with an execution time of : " + 
				(endTime-startTime)/1000 + "seconds.");
		
		return results;
	}
}