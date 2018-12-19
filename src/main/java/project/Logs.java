package project;

public class Logs {

	String DAY ;
	
	String HOUR;
	
	int PV;
	
	int UV;
	
	int IP;
	
	int Newuser;
	
	int VisitTimes;
	
	Double Avgpv;
	
	Double Avgvisittimes;

	public Logs() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Logs(String dAY, String hOUR, int pV, int uV, int iP, int newuser,
			int visitTimes, Double avgpv, Double avgvisittimes) {
		super();
		DAY = dAY;
		HOUR = hOUR;
		PV = pV;
		UV = uV;
		IP = iP;
		Newuser = newuser;
		VisitTimes = visitTimes;
		Avgpv = avgpv;
		Avgvisittimes = avgvisittimes;
	}

	public String getDAY() {
		return DAY;
	}

	public void setDAY(String dAY) {
		DAY = dAY;
	}

	public String getHOUR() {
		return HOUR;
	}

	public void setHOUR(String hOUR) {
		HOUR = hOUR;
	}

	public int getPV() {
		return PV;
	}

	public void setPV(int pV) {
		PV = pV;
	}

	public int getUV() {
		return UV;
	}

	public void setUV(int uV) {
		UV = uV;
	}

	public int getIP() {
		return IP;
	}

	public void setIP(int iP) {
		IP = iP;
	}

	public int getNewuser() {
		return Newuser;
	}

	public void setNewuser(int newuser) {
		Newuser = newuser;
	}

	public int getVisitTimes() {
		return VisitTimes;
	}

	public void setVisitTimes(int visitTimes) {
		VisitTimes = visitTimes;
	}

	public Double getAvgpv() {
		return Avgpv;
	}

	public void setAvgpv(Double avgpv) {	
		Avgpv = avgpv;
	}

	public Double getAvgvisittimes() {
		return Avgvisittimes;
	}

	public void setAvgvisittimes(Double avgvisittimes) {
		Avgvisittimes = avgvisittimes;
	}

	
}
