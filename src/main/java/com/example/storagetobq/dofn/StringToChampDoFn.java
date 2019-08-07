package com.example.storagetobq.dofn;

import org.apache.beam.sdk.transforms.DoFn;

import com.example.storagetobq.domain.Champ;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringToChampDoFn extends DoFn<String, Champ> {

    private static final long serialVersionUID = 78471626844184217L;

    @ProcessElement
    public void processElement(ProcessContext c, OutputReceiver<Champ> out) {
        log.info(c.element());
		if(c.element() != null) {
			String[] splitted = c.element().split(",");
			if(splitted[0].equals("name") && splitted[1].equals("id")) {
				log.info("header => "+c.element());
			} else {
		        Champ champ = new Champ();
		        champ.setName(splitted[0]);
		        champ.setIdChamp(Integer.valueOf(splitted[1]));
			    out.output(champ);
			}
		}
    }

}
