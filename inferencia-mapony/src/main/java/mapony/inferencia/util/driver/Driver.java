// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.util.driver;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import mapony.inferencia.jobs.InferenciaCustomJob;
import mapony.inferencia.jobs.cartodb.CartoDB;
import mapony.inferencia.jobs.groupNear.GroupNear;
import mapony.inferencia.jobs.groupNear.bycity.GroupNearByCity;
import mapony.inferencia.jobs.load.Load;
import mapony.inferencia.jobs.precission.Precission;
import mapony.inferencia.jobs.precission.bycity.PrecissionByCity;
import mapony.inferencia.util.cte.InferenciaCte;

/**
 * @author Alvaro Sanchez Blasco
 * Migrated from Hadoop ProgramDriver (reflection-based) to a plain
 * Map-based dispatcher using Java 8 Supplier lambdas.
 */
public class Driver {

    // Ordered so --help output lists jobs in pipeline execution order.
    private final Map<String, Supplier<InferenciaCustomJob>> jobs = new LinkedHashMap<>();

    public Driver() {
        jobs.put("groupNear",        GroupNear::new);
        jobs.put("groupNearCities",  GroupNearByCity::new);
        jobs.put("precission",       Precission::new);
        jobs.put("precissionCities", PrecissionByCity::new);
        jobs.put("load",             Load::new);
        jobs.put("cartodb",          CartoDB::new);
    }

    public static void main(String[] args) throws Exception {
        new Driver().run(args);
    }

    private void run(String[] args) {
        if (args.length < 2) {
            printUsage();
            System.exit(InferenciaCte.FAIL);
        }
        String name = args[0];
        Supplier<InferenciaCustomJob> supplier = jobs.get(name);
        if (supplier == null) {
            System.out.println("Unknown job: '" + name + "'");
            printUsage();
            System.exit(InferenciaCte.FAIL);
        }
        // Pass remaining args (args[1..]) to the job — first remaining arg is the properties file path
        String[] jobArgs = Arrays.copyOfRange(args, 1, args.length);
        System.exit(supplier.get().execute(jobArgs));
    }

    private void printUsage() {
        System.out.println("Usage: <job-name> <config.properties>");
        System.out.println("Valid job names: " + String.join(", ", jobs.keySet()));
    }
}
