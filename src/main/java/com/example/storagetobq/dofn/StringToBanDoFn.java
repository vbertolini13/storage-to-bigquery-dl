package com.example.storagetobq.dofn;

import org.apache.beam.sdk.transforms.DoFn;

import com.example.storagetobq.domain.Ban;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringToBanDoFn extends DoFn<String, Ban> {

    private static final long serialVersionUID = 78471626844184217L;

    @ProcessElement
    public void processElement(ProcessContext c, OutputReceiver<Ban> out) {
        log.info(c.element());
        String element = c.element().replaceAll("\"", "");
		if(c.element() != null) {
			String[] splitted = element.split(",");
			if(splitted[0].equals("matchid") && splitted[1].equals("teamid") && splitted[2].equals("championid") && splitted[3].equals("banturn")) {
				log.info("header => "+c.element());
			} else {
				Ban ban = new Ban();
				ban.setMatchId(Integer.valueOf(splitted[0]));
				ban.setTeamId(Integer.valueOf(splitted[1]));
				ban.setChampId(Integer.valueOf(splitted[2]));
				ban.setBanTurn(Integer.valueOf(splitted[3]));
			    out.output(ban);
			}
		}
    }

}
