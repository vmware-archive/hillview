package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.api.ITable;
import org.hillview.utils.CsvFileObject;

import java.io.IOException;

public class LoadFileMapper implements IMap<CsvFileObject, ITable> {
    @Override
    public ITable apply(CsvFileObject csvFileObject) {
        try {
            System.out.println("Loading " + csvFileObject);
            return csvFileObject.loadTable();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}