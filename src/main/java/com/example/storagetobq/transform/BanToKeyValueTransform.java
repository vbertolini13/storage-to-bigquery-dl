package com.example.storagetobq.transform;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.example.storagetobq.dofn.BanToKeyValueDoFn;
import com.example.storagetobq.domain.Ban;



public class BanToKeyValueTransform extends PTransform<PCollection<Ban>,PCollection<KV<String, Ban>>>{

    /**
     * 
     */
    private static final long serialVersionUID = 32857647873620144L;

    @Override
	public PCollection<KV<String, Ban>> expand(PCollection<Ban> input) {
		return input.apply(ParDo.of(new BanToKeyValueDoFn()));
	}
}
