package com.flink.bean;

public class VisitorStats {

    //统计开始时间
    private String stt;
    //统计结束时间
    private String edt;
    //维度：版本
    private String vc;
    //维度：渠道
    private String ch;
    //维度：地区
    private String ar;
    //维度：新老用户标识
    private String is_new;
    //度量：独立访客数
    private Long uv_ct=0L;
    //度量：页面访问数
    private Long pv_ct=0L;
    //度量： 进入次数
    private Long sv_ct=0L;
    //度量： 跳出次数
    private Long uj_ct=0L;
    //度量： 持续访问时间
    private Long dur_sum=0L;
    //统计时间
    private Long ts;

    public VisitorStats(String stt, String edt, String vc, String ch, String ar, String is_new, Long uv_ct, Long pv_ct, Long sv_ct, Long uj_ct, Long dur_sum, Long ts) {
        this.stt = stt;
        this.edt = edt;
        this.vc = vc;
        this.ch = ch;
        this.ar = ar;
        this.is_new = is_new;
        this.uv_ct = uv_ct;
        this.pv_ct = pv_ct;
        this.sv_ct = sv_ct;
        this.uj_ct = uj_ct;
        this.dur_sum = dur_sum;
        this.ts = ts;
    }

    public String getStt() {
        return stt;
    }

    public void setStt(String stt) {
        this.stt = stt;
    }

    public String getEdt() {
        return edt;
    }

    public void setEdt(String edt) {
        this.edt = edt;
    }

    public String getVc() {
        return vc;
    }

    public void setVc(String vc) {
        this.vc = vc;
    }

    public String getCh() {
        return ch;
    }

    public void setCh(String ch) {
        this.ch = ch;
    }

    public String getAr() {
        return ar;
    }

    public void setAr(String ar) {
        this.ar = ar;
    }

    public String getIs_new() {
        return is_new;
    }

    public void setIs_new(String is_new) {
        this.is_new = is_new;
    }

    public Long getUv_ct() {
        return uv_ct;
    }

    public void setUv_ct(Long uv_ct) {
        this.uv_ct = uv_ct;
    }

    public Long getPv_ct() {
        return pv_ct;
    }

    public void setPv_ct(Long pv_ct) {
        this.pv_ct = pv_ct;
    }

    public Long getSv_ct() {
        return sv_ct;
    }

    public void setSv_ct(Long sv_ct) {
        this.sv_ct = sv_ct;
    }

    public Long getUj_ct() {
        return uj_ct;
    }

    public void setUj_ct(Long uj_ct) {
        this.uj_ct = uj_ct;
    }

    public Long getDur_sum() {
        return dur_sum;
    }

    public void setDur_sum(Long dur_sum) {
        this.dur_sum = dur_sum;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "VisitorStats{" +
                "stt='" + stt + '\'' +
                ", edt='" + edt + '\'' +
                ", vc='" + vc + '\'' +
                ", ch='" + ch + '\'' +
                ", ar='" + ar + '\'' +
                ", is_new='" + is_new + '\'' +
                ", uv_ct=" + uv_ct +
                ", pv_ct=" + pv_ct +
                ", sv_ct=" + sv_ct +
                ", uj_ct=" + uj_ct +
                ", dur_sum=" + dur_sum +
                ", ts=" + ts +
                '}';
    }
}
