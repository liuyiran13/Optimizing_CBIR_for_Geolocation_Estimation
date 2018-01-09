package MyAPI.General;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import jsat.DataSet;
import jsat.classifiers.DataPoint;
import jsat.clustering.ClustererBase;
import jsat.distributions.empirical.kernelfunc.EpanechnikovKF;
import jsat.distributions.empirical.kernelfunc.GaussKF;
import jsat.distributions.empirical.kernelfunc.KernelFunction;
import jsat.distributions.multivariate.MetricKDE;
import jsat.distributions.multivariate.MultivariateKDE;
import jsat.distributions.multivariate.ProductKDE;
import jsat.exceptions.FailedToFitException;
import jsat.linear.DenseVector;
import jsat.linear.Vec;
import jsat.linear.VecPaired;
import jsat.linear.distancemetrics.DistanceMetric;
import jsat.linear.distancemetrics.EuclideanDistance;
import jsat.utils.PoisonRunnable;
import jsat.utils.RunnableConsumer;
import static jsat.utils.SystemInfo.LogicalCores;


/**
 * The MeanShift algorithms performs clustering on a data set by letting the 
 * data speak for itself and performing a mode search amongst the data set, 
 * returning a cluster for each discovered mode. <br>
 * <br>
 * While not normally discussed in the context of Mean Shift, this 
 * implementation has rudimentary outlier-removal, outliers will not be included
 * in the clustering. <br>
 * <br>
 * The mean shift requires a {@link MultivariateKDE} to run. Contrary to use in 
 * density estimation, where the {@link KernelFunction} used has only a minor 
 * impact on the results, it is highly recommended you use the {@link GaussKF} 
 * for the MeanShift method. This is because of the large support and better 
 * behaved derivative, which adds in the avoidance of oscillating convergence. 
 * <br><br>
 * Implementation Note: This implementation does not snap the values to a grid. 
 * This causes the prior noted oscillation in convergence.  
 * 
 * @author Edward Raff
 */
public class MeanShift_my extends ClustererBase
{
    /**
     * The default number of {@link #getMaxIterations() } is {@value #DefaultMaxIterations}
     */
    public static final int DefaultMaxIterations = 1000;
    /**
     * The default value of {@link #getScaleBandwidthFactor() } is {@value #DefaultScaleBandwidthFactor}
     */
    public static final double DefaultScaleBandwidthFactor = 1.0;
    private MetricKDE mkde;
    private int maxIterations = DefaultMaxIterations;
    private double scaleBandwidthFactor = DefaultScaleBandwidthFactor;

    /**
     * Creates a new MeanShift clustering object using a {@link MetricKDE}, 
     * the {@link GaussKF}, and the {@link EuclideanDistance}.
     */
    public MeanShift_my()
    {
        this(new EuclideanDistance());
    }

    /**
     * Creates a new MeanShift clustering object using a {@link MetricKDE} and 
     * the {@link GaussKF}.
     * @param dm the distance metric to use
     */
    public MeanShift_my(DistanceMetric dm)
    {
        this(new MetricKDE(GaussKF.getInstance(), dm));
    }
    
    public MeanShift_my(DistanceMetric dm, KernelFunction kf )
    {
        this(new MetricKDE(kf, dm));
    }

    KernelFunction DEFAULT_KF = EpanechnikovKF.getInstance();
    /**
     * Creates a new MeanShift clustering object.
     * <br>
     * NOTE: {@link ProductKDE} does not currently support the functions needed to work with MeanShift. 
     * @param mkde the KDE to use in the clustering process. 
     */
    public MeanShift_my(MetricKDE mkde)
    {
        this.mkde = mkde;
    }

    /**
     * Sets the maximum number of iterations the algorithm will go through, 
     * terminating early if convergence has not occurred.
     * @param maxIterations the maximum number of iterations
     * @throws ArithmeticException if a value less than 1 is given
     */
    public void setMaxIterations(int maxIterations)
    {
        if(maxIterations <= 0)
            throw new ArithmeticException("Invalid iteration count, " + maxIterations);
        this.maxIterations = maxIterations;
    }

    /**
     * Returns the maximum number of iterations the algorithm will go through, 
     * terminating early if convergence has not occurred.
     * @return the maximum number of iterations
     */
    public int getMaxIterations()
    {
        return maxIterations;
    }

    /**
     * Sets the value by which the bandwidth of the {@link MultivariateKDE} will
     * be scaled by. 
     * @param scaleBandwidthFactor the value to scale bandwidth by
     * @throws ArithmeticException if the value given is {@link Double#NaN NaN }
     * or {@link Double#POSITIVE_INFINITY infinity}
     */
    public void setScaleBandwidthFactor(double scaleBandwidthFactor)
    {
        if(Double.isNaN(scaleBandwidthFactor) || Double.isInfinite(scaleBandwidthFactor))
            throw new ArithmeticException("Invalid scale factor, " + scaleBandwidthFactor);
        this.scaleBandwidthFactor = scaleBandwidthFactor;
    }

    /**
     * Returns  the value by which the bandwidth of the {@link MultivariateKDE} will
     * be scaled by. 
     * @return the value to scale bandwidth by
     */
    public double getScaleBandwidthFactor()
    {
        return scaleBandwidthFactor;
    }
    
    @Override
    public int[] cluster(DataSet dataSet, int[] designations)
    {
        return cluster(dataSet, null, designations);
    }

    @SuppressWarnings({ })
	public int[] cluster(DataSet dataSet, ExecutorService threadpool, int[] designations, double bandwith)
    {
        try
        {
            if(designations == null || designations.length < dataSet.getSampleSize())
                designations = new int[dataSet.getSampleSize()];
            boolean[] converged = new boolean[dataSet.getSampleSize()];
            Arrays.fill(converged, false);
            
            final KernelFunction k = mkde.getKernelFunction();
            if(threadpool == null){
            	List<DataPoint> dataPoints=dataSet.getDataPoints();
            	List<Vec> dataSet_vec = new ArrayList<Vec>(dataPoints.size());
            	for(DataPoint dp : dataPoints)
            		dataSet_vec.add(dp.getNumericalValues());
                mkde.setUsingData(dataSet_vec,bandwith);
            }
            else
                mkde.setUsingData(dataSet, threadpool);
            System.out.println("band estimated: "+mkde.getBandwith());
            mkde.scaleBandwidth(scaleBandwidthFactor);
            System.out.println("band used: "+mkde.getBandwith());
            
            Vec scratch = new DenseVector(dataSet.getNumNumericalVars());
            Vec[] xit = new Vec[converged.length];
            for(int i = 0; i < xit.length; i++)
                xit[i] = dataSet.getDataPoint(i).getNumericalValues().clone();
            if(threadpool == null)
                mainLoop(converged, xit, designations, scratch, k);
            else
                mainLoop(converged, xit, designations, k, threadpool);
            
            assignmentStep(converged, xit, designations);
            
            return designations;
        }
        catch (InterruptedException ex)
        {
            Logger.getLogger(MeanShift_my.class.getName()).log(Level.SEVERE, null, ex);
            throw new FailedToFitException(ex);
        }
        catch (BrokenBarrierException ex)
        {
            Logger.getLogger(MeanShift_my.class.getName()).log(Level.SEVERE, null, ex);
            throw new FailedToFitException(ex);
        }
    }
    
    @Override
    public int[] cluster(DataSet dataSet, ExecutorService threadpool, int[] designations)
    {
        try
        {
            if(designations == null || designations.length < dataSet.getSampleSize())
                designations = new int[dataSet.getSampleSize()];
            boolean[] converged = new boolean[dataSet.getSampleSize()];
            Arrays.fill(converged, false);
            
            final KernelFunction k = mkde.getKernelFunction();
            if(threadpool == null)
                mkde.setUsingData(dataSet);
            else
                mkde.setUsingData(dataSet, threadpool);
//            System.out.println("band estimated: "+mkde.getBandwith());//for test
            mkde.scaleBandwidth(scaleBandwidthFactor);
//            System.out.println("band used: "+mkde.getBandwith()); //for test
            
            Vec scratch = new DenseVector(dataSet.getNumNumericalVars());
            Vec[] xit = new Vec[converged.length];
            for(int i = 0; i < xit.length; i++)
                xit[i] = dataSet.getDataPoint(i).getNumericalValues().clone();
            if(threadpool == null)
                mainLoop(converged, xit, designations, scratch, k);
            else
                mainLoop(converged, xit, designations, k, threadpool);
            
            assignmentStep(converged, xit, designations);
            
            return designations;
        }
        catch (InterruptedException ex)
        {
            Logger.getLogger(MeanShift_my.class.getName()).log(Level.SEVERE, null, ex);
            throw new FailedToFitException(ex);
        }
        catch (BrokenBarrierException ex)
        {
            Logger.getLogger(MeanShift_my.class.getName()).log(Level.SEVERE, null, ex);
            throw new FailedToFitException(ex);
        }
    }

    private void assignmentStep(boolean[] converged, Vec[] xit, int[] designations)
    {
        //We now repurpose the 'converged' array to indicate if the point has not yet been asigned to a cluster
        
        //Loop through and asign clusters
        int curClusterID = 0;
        boolean progress = true;
        while(progress)
        {
            progress = false;
            int basePos = 0;//This will be the mode of our cluster
            while(basePos < converged.length && !converged[basePos])
                basePos++;
            for(int i = basePos; i < converged.length; i++)
            {
                if(!converged[i]) //ori: if(!converged[i] || designations[i] == -1), this will cause: for data with designations[i] == -1, will dead loop! its converged[i] will be true forever!!
                    continue;//Already assigned
                progress = true;
                double kk=Math.abs(xit[basePos].pNormDist(2, xit[i]));
                if(kk < 1e-3)
                {
                    converged[i] = false;
                    designations[i] = curClusterID;
                }
            }
            
            curClusterID++;
        }
    }

    private void mainLoop(boolean[] converged, Vec[] xit, int[] designations, Vec scratch, final KernelFunction k)
    {
        boolean progress = true;
        
        int count = 0;
        
        while(progress && count++ < maxIterations)
        {
            progress = false;
            
            for(int i = 0; i < converged.length; i++)
            {
                if(converged[i])
                    continue;
                progress = true;
                
                convergenceStep(xit, i, converged, designations, scratch, k);
            }
        }
        
        //Fill b/c we may have bailed out due to maxIterations
        Arrays.fill(converged, true);
    }
    
    private void mainLoop(final boolean[] converged, final Vec[] xit, final int[] designations, final KernelFunction k, ExecutorService ex) throws InterruptedException, BrokenBarrierException
    {
        boolean progress = true;
        
        int count = 0;
        /*
         * +1 b/c we have to wait for the worker threads, but we also want this 
         * calling thread to wait with them. Hence, +1
         */
        CyclicBarrier barrier = new CyclicBarrier(LogicalCores+1);
        
        final BlockingQueue<Runnable> jobs = new ArrayBlockingQueue<Runnable>(LogicalCores*2);
        
        final ThreadLocal<Vec> localScratch = new ThreadLocal<Vec>()
        {
            @Override
            protected Vec initialValue()
            {
                return new DenseVector(xit[0].length());
            }
        };
        
        while(progress && count++ < maxIterations)
        {
            progress = false;
            
            for(int i = 0; i < LogicalCores; i++)
                ex.submit(new RunnableConsumer(jobs));
            
            for(int i = 0; i < converged.length; i++)
            {
                if(converged[i])
                    continue;
                progress = true;
                final int ii = i;
                
                jobs.put(new Runnable() {

                    @Override
                    public void run()
                    {
                        convergenceStep(xit, ii, converged, designations, localScratch.get(), k);
                    }
                });
                
            }
            
            for(int i = 0; i < LogicalCores; i++)
                jobs.put(new PoisonRunnable(barrier));
            barrier.await();
            barrier.reset();
        }
        
        //Fill b/c we may have bailed out due to maxIterations
        Arrays.fill(converged, true);
    }
    
    /**
     * Computes the meanShift of the point at the given index, and then updates the vector array to indicate the movement towards the mode for the data point. 
     * @param xit the array of the current data point's positions
     * @param i the index of the data point being considered
     * @param converged the array used to indicate the convergence to a mode
     * @param designations the array to store value designations in
     * @param scratch the vector to compute work with
     * @param k the kernel function to use
     */
    private void convergenceStep(final Vec[] xit, int i, final boolean[] converged, final int[] designations, final Vec scratch, final KernelFunction k)
    {
        double denom = 0.0;
        Vec xCur = xit[i];
        List<VecPaired<VecPaired<Vec, Integer>, Double>> contrib = mkde.getNearbyRaw(xCur); //neighours, include itself
        
        if(contrib.size() == 1)
        {
            //If a point has no neighbors, it can not shift, and is its own mdoe - so we mark it noise
            converged[i] = true;
            designations[i] = -1;
        }
        else
        {
            scratch.zeroOut();
            for(VecPaired<VecPaired<Vec, Integer>, Double> v : contrib)
            {
                double g = - k.kPrime(v.getPair());
                denom += g;
                scratch.mutableAdd(g, v);
            }
            
            if(denom!=0){
            	scratch.mutableDivide(denom);
                if( Math.abs(scratch.pNormDist(2, xCur)) < 1e-5)
                    converged[i] = true;
                scratch.copyTo(xCur);
            }else{// neighbour all the same!
            	converged[i] = true;
            }
            
            /* ori: if neighbour all the same, denom will be 0, scratch will be NAN!!
             * scratch.mutableDivide(denom);
                if( Math.abs(scratch.pNormDist(2, xCur)) < 1e-5)
                    converged[i] = true;
                scratch.copyTo(xCur);
             */
            
        }
    }
    
}
