package com.example.storagetobq.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import com.example.storagetobq.domain.Champ;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChampToKeyValueDoFn extends DoFn<Champ, KV<String, Champ>> {

    private static final long serialVersionUID = 78471626844184217L;

    @ProcessElement
	public void processElement(ProcessContext c) {
    	log.info(c.element().getIdChamp().toString());
		c.output(KV.of(c.element().getIdChamp().toString(), c.element()));
	}

}
