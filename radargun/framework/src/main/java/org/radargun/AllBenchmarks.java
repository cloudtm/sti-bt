package org.radargun;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AllBenchmarks {

    public static final int[] POSSIBLE_NODES = { 20, 40, 60, 80 };
    public static final String[] ALGS = { "gmu", "twc" };
    public static final String[] BENCHS = { "ycsb" };
    public static final String[] TOS = { "dl", "to" };
    public static final int ATTEMPTS = 1;

    public static class Result {
        public List<Double> throughput = new ArrayList<Double>();
        public List<Double> aborts = new ArrayList<Double>();

        public Double getThroughputAvg() {
            Double tmp = 0.0;
            for (Double t : throughput) {
                tmp += t;
            }
            return tmp / ATTEMPTS;
        }

        public Double getThroughputDeviation() {
            Double avg = getThroughputAvg();
            Double variance = 0.0;
            for (Double t : throughput) {
                variance += Math.pow(Math.abs(avg - t), 2);
            }
            variance = variance / ATTEMPTS;
            return Math.sqrt(variance);
        }

        public Double getAbortDeviation() {
            Double avg = getAbortAvg();
            Double variance = 0.0;
            for (Double a : aborts) {
                variance += Math.pow(Math.abs(avg - a), 2);
            }
            variance = variance / ATTEMPTS;
            return Math.sqrt(variance);
        }

        public Double getAbortAvg() {
            Double tmp = 0.0;
            for (Double a : aborts) {
                tmp += a;
            }
            return tmp / ATTEMPTS;
        }
    }

    public static void main(String[] args) {
//            gmuVersusTWC(args, 1);
//        totalOrder(args);
            normalWorkloadWithTO(args);
    }

    // normal workload and what TO does on top of it
    protected static void normalWorkloadWithTO(String[] args) {
        // Benchmark -> Nodes -> Algorithm -> totalOrder
        Map<String, Map<Integer, Map<String, Map<Boolean, Result>>>> allData = new HashMap<String, Map<Integer, Map<String, Map<Boolean, Result>>>>();
        for (String benchmark : BENCHS) {
            Map<Integer, Map<String, Map<Boolean, Result>>> perBenchmark = new HashMap<Integer, Map<String, Map<Boolean, Result>>>();
            for (int nodes : POSSIBLE_NODES) {
                Map<String, Map<Boolean, Result>> perNodes = new HashMap<String, Map<Boolean, Result>>();
                for (String alg : ALGS) {
                    Map<Boolean, Result> perTO = new HashMap<Boolean, Result>();
                    for (String to : TOS) {
                        Result result = new Result();
                        for (int a = 0; a < ATTEMPTS; a++) {
                            List<String> content;
                            if (to.equals("to")) {
                                content = getFileContent(args[0] + "/" + benchmark + "-" + alg + "-to-" + nodes + "-" + (a+1) + ".csv");
                            } else {
                                content = getFileContent(args[0] + "/" + benchmark + "-" + alg + "-dl-" + nodes + "-" + (a+1) + ".csv");
                            }
                            content.remove(0);
                            double throughput = 0.0;
                            int ab = 0;
                            for (String line : content) {
                                String[] parts = line.split(",");
                                if (benchmark.equals("tpcc")) {
                                    throughput += Double.parseDouble(parts[2]);
                                    ab += Integer.parseInt(parts[11]);                                    
                                } else {
                                    throughput += Double.parseDouble(parts[1]);
                                    ab += Integer.parseInt(parts[2]);
                                }
                            }
                            result.throughput.add(throughput);
                            result.aborts.add((double)ab);
                        }
                        boolean isTO = (to.equals("to"));
                        perTO.put(isTO, result);
                    }
                    perNodes.put(alg, perTO);
                }
                perBenchmark.put(nodes, perNodes);
            }
            allData.put(benchmark, perBenchmark);
        }
        
        for (String benchmark : BENCHS) {
            String output = "- gmu twc gmuTO twcTO";
            String outputA = "- gmu twc gmuTO twcTO";
            for (int nodes : POSSIBLE_NODES) {
                output += "\n" + nodes;
                outputA += "\n" + nodes;
                Result gmu = allData.get(benchmark).get(nodes).get("gmu").get(false);
                Result gmuTO = allData.get(benchmark).get(nodes).get("gmu").get(true);
                Result twc = allData.get(benchmark).get(nodes).get("twc").get(false);
                Result twcTO = allData.get(benchmark).get(nodes).get("twc").get(true);
                double avgGMU = gmu.getThroughputAvg();
                double devGMU = gmu.getThroughputDeviation();
                double abortsGMU = gmu.getAbortAvg();
                double avgGMUTO = gmuTO.getThroughputAvg();
                double devGMUTO = gmuTO.getThroughputDeviation();
                double abortsGMUTO = gmuTO.getAbortAvg();
                double avgTWC = twc.getThroughputAvg();
                double devTWC = twc.getThroughputDeviation();
                double abortsTWC = twc.getAbortAvg();
                double avgTWCTO = twcTO.getThroughputAvg();
                double devTWCTO = twcTO.getThroughputDeviation();
                double abortsTWCTO = twcTO.getAbortAvg();
                output += " " + roundTwoDecimals(avgGMU) + " " + roundTwoDecimals(avgTWC) + " " + roundTwoDecimals(avgGMUTO) + " " + roundTwoDecimals(avgTWCTO);
                outputA += " " + roundTwoDecimals(abortsGMU) + " " + roundTwoDecimals(abortsTWC) + " " + roundTwoDecimals(abortsGMUTO) + " " + roundTwoDecimals(abortsTWCTO);
//                output += " " + roundTwoDecimals(avgTWC) + " " + roundTwoDecimals(avgTWCTO);
//                outputA += " " + roundTwoDecimals(abortsTWC) + " " + roundTwoDecimals(abortsTWCTO);
            }
            writeToFile(args[0] + "/results/to-" + benchmark + "-throughput.output", output);
            writeToFile(args[0] + "/results/to-" + benchmark + "-aborts.output", outputA);
        }
    }
    
    // the workload here is always prone to deadlocks
    protected static void totalOrder(String[] args) {
        // Benchmark -> Nodes -> Algorithm -> totalOrder
        Map<String, Map<Integer, Map<String, Map<Boolean, Result>>>> allData = new HashMap<String, Map<Integer, Map<String, Map<Boolean, Result>>>>();
        for (String benchmark : BENCHS) {
            Map<Integer, Map<String, Map<Boolean, Result>>> perBenchmark = new HashMap<Integer, Map<String, Map<Boolean, Result>>>();
            for (int nodes : POSSIBLE_NODES) {
                Map<String, Map<Boolean, Result>> perNodes = new HashMap<String, Map<Boolean, Result>>();
                for (String alg : ALGS) {
                    Map<Boolean, Result> perTO = new HashMap<Boolean, Result>();
                    for (String to : TOS) {
                        Result result = new Result();
                        for (int a = 0; a < ATTEMPTS; a++) {
                            List<String> content = getFileContent(args[0] + "/" + benchmark + "-" + alg + "-spread-" + to + "-" + nodes + "-" + (a+1) + ".csv");
                            content.remove(0);
                            double throughput = 0.0;
                            int ab = 0;
                            for (String line : content) {
                                String[] parts = line.split(",");
                                throughput += Double.parseDouble(parts[1]);
                                ab += Integer.parseInt(parts[2]);
                            }
                            result.throughput.add(throughput);
                            result.aborts.add((double)ab);
                        }
                        boolean isTO = (to.equals("to"));
                        perTO.put(isTO, result);
                    }
                    perNodes.put(alg, perTO);
                }
                perBenchmark.put(nodes, perNodes);
            }
            allData.put(benchmark, perBenchmark);
        }
        
        for (String benchmark : BENCHS) {
            String output = "- gmu twc gmuTO twcTO";
            String outputA = "- gmu twc gmuTO twcTO";
            for (int nodes : POSSIBLE_NODES) {
                output += "\n" + nodes;
                outputA += "\n" + nodes;
                Result gmu = allData.get(benchmark).get(nodes).get("gmu").get(false);
                Result twc = allData.get(benchmark).get(nodes).get("twc").get(false);
                Result gmuTO = allData.get(benchmark).get(nodes).get("gmu").get(true);
                Result twcTO = allData.get(benchmark).get(nodes).get("twc").get(true);
                double avgGMU = gmu.getThroughputAvg();
                double avgTWC = twc.getThroughputAvg();
                double devGMU = gmu.getThroughputDeviation();
                double devTWC = twc.getThroughputDeviation();
                double abortsGMU = gmu.getAbortAvg();
                double abortsTWC = twc.getAbortAvg();
                double avgGMUTO = gmuTO.getThroughputAvg();
                double avgTWCTO = twcTO.getThroughputAvg();
                double devGMUTO = gmuTO.getThroughputDeviation();
                double devTWCTO = twcTO.getThroughputDeviation();
                double abortsGMUTO = gmuTO.getAbortAvg();
                double abortsTWCTO = twcTO.getAbortAvg();
                output += " " + roundTwoDecimals(avgGMU) + " " + roundTwoDecimals(avgTWC) + " " + roundTwoDecimals(avgGMUTO) + " " + roundTwoDecimals(avgTWCTO);
                outputA += " " + roundTwoDecimals(abortsGMU) + " " + roundTwoDecimals(abortsTWC) + " " + roundTwoDecimals(abortsGMUTO) + " " + roundTwoDecimals(abortsTWCTO);
            }
            writeToFile(args[0] + "/results/to-" + benchmark + "-throughput.output", output);
            writeToFile(args[0] + "/results/to-" + benchmark + "-aborts.output", outputA);
        }
    }
    
    protected static void gmuVersusTWC(String[] args, int pos) {
        // Benchmark -> Nodes -> Algorithm
        Map<String, Map<Integer, Map<String, Result>>> allData = new HashMap<String, Map<Integer, Map<String, Result>>>();
        for (String benchmark : BENCHS) {
            Map<Integer, Map<String, Result>> perBenchmark = new HashMap<Integer, Map<String, Result>>();
            for (int nodes : POSSIBLE_NODES) {
                Map<String, Result> perNodes = new HashMap<String, Result>();
                for (String alg : ALGS) {
                    Result result = new Result();
                    for (int a = 0; a < ATTEMPTS; a++) {
                        List<String> content = getFileContent(args[0] + "/" + benchmark + "-" + alg + "-to-" + nodes + "-" + (1) + ".csv");
                        content.remove(0);
                        double throughput = 0.0;
                        int ab = 0;
                        for (String line : content) {
                            String[] parts = line.split(",");
                            if (benchmark.equals("tpcc")) {
                                throughput += Double.parseDouble(parts[2]);
                                ab += Integer.parseInt(parts[11]);                                    
                            } else {
                                throughput += Double.parseDouble(parts[1]);
                                ab += Integer.parseInt(parts[2]);
                            }
                        }
                        result.throughput.add(throughput);
                        result.aborts.add((double)ab);
                    }
                    perNodes.put(alg, result);
                }
                perBenchmark.put(nodes, perNodes);
            }
            allData.put(benchmark, perBenchmark);
        }

        for (String benchmark : BENCHS) {
            String output = "- gmu twc";
            String outputA = "- gmu twc";
            for (int nodes : POSSIBLE_NODES) {
                output += "\n" + nodes;
                outputA += "\n" + nodes;
                Result gmu = allData.get(benchmark).get(nodes).get("gmu");
                Result twc = allData.get(benchmark).get(nodes).get("twc");
                double avgTWC = twc.getThroughputAvg();
                double devTWC = twc.getThroughputDeviation();
                double abortsTWC = twc.getAbortAvg();
                double devAbortsTWC = twc.getAbortDeviation();
                double avgGMU = gmu.getThroughputAvg();
                double devGMU = gmu.getThroughputDeviation();
                double abortsGMU = gmu.getAbortAvg();
                double devAbortsGMU = gmu.getAbortDeviation();
                output += " " + roundTwoDecimals(avgGMU) + " " + roundTwoDecimals(devGMU) + " " + roundTwoDecimals(avgTWC) + " " + roundTwoDecimals(devTWC);
                outputA += " " + roundTwoDecimals(abortsGMU) + " " + roundTwoDecimals(devAbortsGMU) + " " + roundTwoDecimals(abortsTWC) + " " + roundTwoDecimals(devAbortsTWC);
                
//                output += " " + roundTwoDecimals(avgTWC) + " " + roundTwoDecimals(devTWC);
//                outputA += " " + roundTwoDecimals(abortsTWC) + " " + roundTwoDecimals(devAbortsTWC);
            }
            writeToFile(args[0] + "/results/" + benchmark + "-throughput.output", output);
            writeToFile(args[0] + "/results/" + benchmark + "-aborts.output", outputA);
        }
    }

    private static void writeToFile(String filename, String content) {
        try {
            FileWriter fstream = new FileWriter(filename);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(content);
            out.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> getFileContent(String filename) {
        List<String> testLines1 = new ArrayList<String>();
        try {
            FileInputStream is = new FileInputStream(filename);
            DataInputStream in = new DataInputStream(is);
            BufferedReader br = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")));
            String strLine;
            while ((strLine = br.readLine()) != null) {
                if (strLine.equals("")) {
                    continue;
                }
                testLines1.add(strLine);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return testLines1;
    }

    private static double roundTwoDecimals(double d) {
        DecimalFormat twoDForm = new DecimalFormat("#");
        return Double.valueOf(twoDForm.format(d));
    }

}
