package com.example.storagetobq.transform;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.example.storagetobq.dofn.StringToBanDoFn;
import com.example.storagetobq.domain.Ban;

public class StringToBanTransform extends PTransform<PCollection<String>, PCollection<Ban>>{

    /**
     * 
     */
    private static final long serialVersionUID = 32857647873620144L;

    @Override
    public PCollection<Ban> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new StringToBanDoFn()));
    }
}
