// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.jobs.precission.bycity;

import org.slf4j.LoggerFactory;

import mapony.inferencia.jobs.precission.Precission;
import mapony.inferencia.util.cte.JobNamesCte;

/**
 * @author Alvaro Sanchez Blasco
 * Migrated to Spark: extends Precission with jobName override.
 * In the Hadoop version this variant routed output per city via
 * MultipleOutputsReducer + CityPartitioner; in Spark both variants
 * produce the same objectFile format and city filtering is done
 * downstream in the CartoDB job.
 */
public class PrecissionByCity extends Precission {

    @Override
    public void setClassLogger() {
        logger = LoggerFactory.getLogger(PrecissionByCity.class);
    }

    @Override
    protected void init() {
        super.init();
        // Override only the job name; all other settings from Precission.init() apply.
        setJobName(JobNamesCte.precissionByCity);
    }

    public static void main(String[] args) throws Exception {
        checkMainClassArgs(args);
        System.exit(new PrecissionByCity().execute(args));
    }
}
