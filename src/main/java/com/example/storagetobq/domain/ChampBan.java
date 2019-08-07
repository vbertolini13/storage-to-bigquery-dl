package com.example.storagetobq.domain;

import java.io.Serializable;

import lombok.Data;

@Data
public class ChampBan implements Serializable {

	private static final long serialVersionUID = -2016898454348556085L;

	private Integer idChamp;
	private String nameChamp;
	private Integer turn;
	private Integer idMatch;
	private Long cantidad;
	
}
