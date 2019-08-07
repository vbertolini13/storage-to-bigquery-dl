package com.example.storagetobq.transform;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.example.storagetobq.dofn.StringToChampDoFn;
import com.example.storagetobq.domain.Champ;

public class StringToChampTransform extends PTransform<PCollection<String>, PCollection<Champ>>{

    /**
     * 
     */
    private static final long serialVersionUID = 32857647873620144L;

    @Override
    public PCollection<Champ> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new StringToChampDoFn()));
    }
}
