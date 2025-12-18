package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.example.analyse.AnalyseJob;
import org.example.sort.SortJob;

@Slf4j
public class Main {
    public static void main(String[] args) throws Exception {
        try {
            validateArgs(args);

            var inputDir = args[0];
            var outputDir = args[1];
            var reducerCount = Integer.parseInt(args[2]);
            var datablockSizeBytes = Integer.parseInt(args[3]) * 1024;
            var intermediateResultDir = outputDir + "-intermediate";

            var configuration = createConfiguration(datablockSizeBytes);

            var startTime = System.currentTimeMillis();

            runJob(configuration, new AnalyseJob(), inputDir, intermediateResultDir, reducerCount);
            runJob(configuration, new SortJob(), intermediateResultDir, outputDir, reducerCount);

            var endTime = System.currentTimeMillis();
            log.info("Задача выполнена за {} мс", endTime - startTime);
        } catch (Exception ex) {
            log.error("Ошибка при выполнении программы: {}", ex.getMessage(), ex);
        }
    }

    private static void validateArgs(String[] args) {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "Не найдены аргументы input, output, reducer_count или datablock_size"
            );
        }
    }

    private static Configuration createConfiguration(int datablockSizeBytes) {
        var configuration = new Configuration();
        configuration.set("mapreduce.input.fileinputformat.split.maxsize", Integer.toString(datablockSizeBytes));
        return configuration;
    }

    private static void runJob(Configuration configuration, Tool job, String input, String output, int reducers) throws Exception {
        var args = new String[]{ input, output, String.valueOf(reducers) };
        var result = ToolRunner.run(configuration, job, args);

        if (result != 0) {
            throw new IllegalStateException("Job завершился с ошибкой, код = " + result);
        }
    }
}