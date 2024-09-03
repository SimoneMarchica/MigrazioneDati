package com.example.demo02.service;

import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;

@Service
public class CsvPostProcessor {

    public void processCsv(String inputFile, String outputFile) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), StandardCharsets.ISO_8859_1));  // Modificato in ISO-8859-1
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.ISO_8859_1))) {  // Modificato in ISO-8859-1

            String line;
            while ((line = reader.readLine()) != null) {
                // Processa ogni riga per rimuovere le virgolette dai campi vuoti
                String processedLine = processLine(line);
                writer.write(processedLine);
                writer.newLine();
            }
        }

        // Elimina il vecchio file di input solo dopo aver creato correttamente quello nuovo
        File oldFile = new File(inputFile);
        if (oldFile.delete()) {
            System.out.println("Vecchio file CSV eliminato con successo.");
        } else {
            System.err.println("Errore durante l'eliminazione del vecchio file CSV.");
        }
    }

    private String processLine(String line) {
        // Usa regex per sostituire campi vuoti con solo il separatore (;)
        // Rimuove virgolette doppie dai campi vuoti e gestisce vari scenari di virgolettatura
        // Mantiene virgolette doppie che fanno parte del contenuto
        return line.replaceAll(";\"\"(?=;)", ";")  // Gestisce campi vuoti in posizione intermedia
            .replaceAll("^\"\";", ";")       // Gestisce campi vuoti all'inizio
            .replaceAll(";\"\"$", ";")       // Gestisce campi vuoti alla fine
            .replaceAll("(?<=[^;])\"\"(?=[^;])", "\""); // Mantiene virgolette doppie all'interno del testo
    }
}
