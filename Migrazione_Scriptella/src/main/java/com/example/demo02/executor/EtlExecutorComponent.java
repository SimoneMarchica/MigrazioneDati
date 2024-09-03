package com.example.demo02.executor;

import com.example.demo02.service.CsvPostProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scriptella.execution.EtlExecutor;
import scriptella.execution.EtlExecutorException;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class EtlExecutorComponent {

    @Autowired
    private CsvPostProcessor csvPostProcessor;

    @PostConstruct
    public void runEtl() throws EtlExecutorException {
        try {
            // Percorso relativo al file ETL del DB Intermedio
             //String etlFilePath = "src/main/resources/etl/EtlCsvToDb.xml";
             //EtlExecutor.newExecutor(new File(etlFilePath)).execute();
             //System.out.println("ETL eseguito con successo.");
            //executeSecondEtl();
             executeThirdEtl();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Errore durante l'esecuzione dell'ETL: " + e.getMessage());
        }
    }

    public void executeSecondEtl() {
        try {
            // Percorso relativo al file ETL del DB finale
            String etlFilePath = "src/main/resources/etl/EtlDbToDb.xml";
            EtlExecutor.newExecutor(new File(etlFilePath)).execute();
            System.out.println("ETL eseguito con successo.");
        } catch (EtlExecutorException e) {
            e.printStackTrace();
        }
    }

    public void executeThirdEtl() {
        try {
            // Percorso relativo al file ETL del DB finale
            String etlFilePath = "src/main/resources/etl/EtlDbToCsv.xml";
            EtlExecutor.newExecutor(new File(etlFilePath)).execute();
            System.out.println("ETL eseguito con successo.");

            // Pianifica il post-processing con un ritardo di 1 minuto
            scheduleCsvPostProcessing();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Errore durante l'esecuzione del terzo ETL o del post-processing: " + e.getMessage());
        }
    }

    private void scheduleCsvPostProcessing() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        // Configura un ritardo di 2 minuti (120 secondi)
        scheduler.schedule(() -> {
            try {
                String inputFile = "C:\\Users\\simone.marchica\\Desktop\\demo02\\src\\main\\resources\\file/output.csv";
                String outputFile = "C:\\Users\\simone.marchica\\Desktop\\demo02\\src\\main\\resources\\file/output_corrected.csv";
                // File CSV corretto
                csvPostProcessor.processCsv(inputFile, outputFile);
                scheduler.shutdown(); // Chiude il scheduler dopo l'esecuzione
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Errore durante il post-processing del CSV: " + e.getMessage());
            }
        }, 1, TimeUnit.MINUTES); // Specifica il ritardo di 1 minuto
    }
}
