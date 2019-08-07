package com.example.storagetobq.domain;

import java.io.Serializable;

import lombok.Data;

@Data
public class Ban implements Serializable {

	private static final long serialVersionUID = -2016898454348556085L;

	private Integer matchId;
	private Integer teamId;
	private Integer champId;
	private Integer banTurn;
	
}
