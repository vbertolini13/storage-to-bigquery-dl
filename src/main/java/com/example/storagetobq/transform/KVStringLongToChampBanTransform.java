package com.example.storagetobq.transform;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.example.storagetobq.dofn.KVStringLongToTableRowDoFn;
import com.google.api.services.bigquery.model.TableRow;

public class KVStringLongToChampBanTransform extends PTransform<PCollection<KV<String, Long>>, PCollection<TableRow>>{

    /**
     * 
     */
    private static final long serialVersionUID = 32857647873620144L;

    @Override
    public PCollection<TableRow> expand(PCollection<KV<String, Long>> input) {
        return input.apply(ParDo.of(new KVStringLongToTableRowDoFn()));
    }
}
