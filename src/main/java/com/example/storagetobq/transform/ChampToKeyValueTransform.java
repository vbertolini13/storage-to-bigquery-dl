package com.example.storagetobq.transform;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.example.storagetobq.dofn.ChampToKeyValueDoFn;
import com.example.storagetobq.domain.Champ;



public class ChampToKeyValueTransform extends PTransform<PCollection<Champ>,PCollection<KV<String, Champ>>>{

    /**
     * 
     */
    private static final long serialVersionUID = 32857647873620144L;

    @Override
	public PCollection<KV<String, Champ>> expand(PCollection<Champ> input) {
		return input.apply(ParDo.of(new ChampToKeyValueDoFn()));
	}
}
