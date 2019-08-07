package com.example.storagetobq.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.example.storagetobq.persistence.ChampBanSchema;
import com.google.api.services.bigquery.model.TableRow;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KVStringLongToTableRowDoFn extends DoFn<KV<String, Long>, TableRow> {

    private static final long serialVersionUID = 78471626844184217L;

    @ProcessElement
    public void processElement(ProcessContext c, OutputReceiver<TableRow> out) {
    	if(c.element().getKey() != null) {

        	String[] sp = c.element().getKey().split("-");
        	log.info("**************************key =>"+c.element().getKey());
        	TableRow row = new TableRow();
            row.set(ChampBanSchema.getTableSchema().getFields().get(0).getName(), sp[0]);
            row.set(ChampBanSchema.getTableSchema().getFields().get(1).getName(), sp[1]);
            row.set(ChampBanSchema.getTableSchema().getFields().get(2).getName(), c.element().getValue());

    		log.info("**************************row =>"+row.toString());
            c.output(row);
    	}
    }

}
