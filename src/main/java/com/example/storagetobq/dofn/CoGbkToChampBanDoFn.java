package com.example.storagetobq.dofn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import com.example.storagetobq.domain.Ban;
import com.example.storagetobq.domain.Champ;
import com.example.storagetobq.domain.ChampBan;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoGbkToChampBanDoFn extends DoFn<KV<String,CoGbkResult>, KV<String, ChampBan>> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7439580932503177413L;
	private TupleTag<Champ> champTupleTag;
	private TupleTag<Ban> banTupleTag;
	
	public CoGbkToChampBanDoFn(TupleTag<Champ> champTupleTag, TupleTag<Ban> banTupleTag) {
		this.champTupleTag=champTupleTag;
		this.banTupleTag=banTupleTag;
	}
    @ProcessElement
	public void processElement(ProcessContext c) {
		Iterable<Champ> itChamps = c.element().getValue().getAll(champTupleTag);
		Iterable<Ban> itBans = c.element().getValue().getAll(banTupleTag);
		
		for(Champ champ : itChamps) {
			for(Ban ban : itBans) {
				if(champ.getIdChamp().equals(ban.getChampId())){
					ChampBan cb = new ChampBan();
					cb.setIdChamp(champ.getIdChamp());
					cb.setNameChamp(champ.getName());
					cb.setTurn(ban.getBanTurn());
					cb.setIdMatch(ban.getMatchId());
					log.info(champ.getIdChamp()+"-"+champ.getName());
					c.output(KV.of(champ.getIdChamp()+"-"+champ.getName(), cb));
				}
			}
		}
	}

}
