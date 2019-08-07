package com.example.storagetobq.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.example.storagetobq.domain.Ban;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BanToKeyValueDoFn extends DoFn<Ban, KV<String, Ban>> {

    private static final long serialVersionUID = 78471626844184217L;

    @ProcessElement
	public void processElement(ProcessContext c) {
    	log.info(c.element().getChampId().toString());
		c.output(KV.of(c.element().getChampId().toString(), c.element()));
	}

}
