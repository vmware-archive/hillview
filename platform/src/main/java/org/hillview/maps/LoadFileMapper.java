package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.api.ITable;
import org.hillview.utils.CsvFileObject;
import org.hillview.utils.HillviewLogManager;

import java.io.IOException;
import java.util.logging.Level;

public class LoadFileMapper implements IMap<CsvFileObject, ITable> {
    @Override
    public ITable apply(CsvFileObject csvFileObject) {
        try {
            HillviewLogManager.instance.logger.log(Level.INFO, "Loading " + csvFileObject);
            return csvFileObject.loadTable();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}