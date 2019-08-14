package com.example.storagetobq.persistence;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

public class ChampBanSchema implements Serializable{

    /**
	 *
	 */
	private static final long serialVersionUID = -1784113519468859296L;

	public static TableSchema getTableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("ID_CHAMPION").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("CHAMPION").setType("STRING"));
        fields.add(new TableFieldSchema().setName("TURN").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("ID_MATCH").setType("INTEGER"));
        return new TableSchema().setFields(fields);
    }

}
