package assignment2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;

public class MatrixSaver implements Serializable
{
	
	/**
	 * Default Serializable Version UID
	 */
	private static final long serialVersionUID = 1L;

	private int star;
	
	private double minDist;
	private Vector<Integer> minDistStars;
	
	private double maxDist;
	private Vector<Integer> maxDistStars;
	
	private double sumDists;
	
	public MatrixSaver(int pStar)
	{
		star = pStar;
		
		minDist = Double.MAX_VALUE;
		minDistStars = new Vector<Integer>();
		
		maxDist = 0.0;
		maxDistStars = new Vector<Integer>();
		
		sumDists = 0.0;
	}
	
	public MatrixSaver(int pStar, 
						   double pMinDist, Vector<Integer> pMinDistStars,
						   double pMaxDist, Vector<Integer> pMaxDistStars,
						   double pSumDists) 
	{
		star = pStar;
		minDist = pMinDist;
		minDistStars = pMinDistStars;
		maxDist = pMaxDist;
		maxDistStars = pMaxDistStars;
		sumDists = pSumDists;
	}
	
	public void updateMinDist(double dist, int otherStar)
	{
		// check if minimal-distant star
		if ( dist < minDist )
			this.minDistStars.clear(); // unique minimum-distant star
		if ( dist <= minDist )
		{
			minDistStars.add(otherStar);
			minDist = dist;
		}
	}
	
	public void updateMinDist(double dist, Vector otherStars)
	{
		// check if minimal-distant star
		if ( dist < minDist )
			this.minDistStars.clear(); // unique minimum-distant star
		if ( dist <= minDist )
		{
			for ( Iterator iter = otherStars.iterator(); iter.hasNext(); )
			{
				minDistStars.add((Integer)iter.next());
			}
			minDist = dist;
		}
	}

	public void updateMaxDist(double dist, int otherStar)
	{
		// check if maximal-distant star
		if ( dist > maxDist )
			maxDistStars.clear(); // unique maximum-distant star
		if ( dist >= maxDist )
		{
			maxDistStars.add(otherStar);
			maxDist = dist;
		}
	}
	
	public void updateMaxDist(double dist, Vector otherStars)
	{
		// check if maximal-distant star
		if ( dist > maxDist )
			maxDistStars.clear(); // unique maximum-distant star
		if ( dist >= maxDist )
		{
			for ( Iterator iter = otherStars.iterator(); iter.hasNext(); )
			{
				maxDistStars.add((Integer)iter.next());
			}
			maxDist = dist;
		}
	}			
	
	public void addToSumDists(double amount)
	{
		sumDists += amount;
	}
	
	public int getStar()
	{
		return star;
	}

	public double getMinDist()
	{
		return minDist;
	}

	public Vector<Integer> getMinDistStars()
	{
		return minDistStars;
	}

	public double getMaxDist()
	{
		return maxDist;
	}

	public Vector<Integer> getMaxDistStars()
	{
		return maxDistStars;
	}

	public double getSumDists()
	{
		return sumDists;
	}

	
}