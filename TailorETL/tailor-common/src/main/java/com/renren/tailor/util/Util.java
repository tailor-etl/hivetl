package com.renren.tailor.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {
  public static final String DELEMITER = "\",\"";
  public static final String HIVE_DELEMITER = "\001";
  public static HashMap<String, Pattern> productPattern = new HashMap<String, Pattern>();
  static {
    productPattern.put("headphoto", Pattern.compile("http://head.upload.renren.com"));
    productPattern.put("profile", Pattern.compile("http://[^\\.]+\\.renren.com/\\d+/profile\\?"));
    productPattern.put("homepage", Pattern.compile("http://[^\\.]+\\.renren.com/\\d+\\?"));
    productPattern.put("app", Pattern.compile("http://m?apps?.renren.com"));
    productPattern.put("connect", Pattern.compile("http://www.connect.renren.com"));
    productPattern.put("foot", Pattern.compile("http://www.renren.com/myfoot.do\\?"));
  }
  public static HashMap<String, String> productString = new HashMap<String, String>();
  static {
    productString.put("photo.renren.com", "photo");
    productString.put("status.renren.com", "status");
    productString.put("share.renren.com", "share");
    productString.put("page.renren.com", "page");
    productString.put("friend.renren.com", "friend");
    productString.put("blog.renren.com", "blog");
    productString.put("music.renren.com", "music");
    productString.put("zhan.renren.com", "zhan");
    productString.put("browse.renren.com", "browse");
    productString.put("guide.renren.com", "guide");
    productString.put("req.renren.com", "req");
    productString.put("sg.renren.com", "sg");
    productString.put("gossip.renren.com", "gossip");
    productString.put("mm.renren.com", "mm");
    productString.put("lover.renren.com", "lover");
    productString.put("i.renren.com", "i");
    productString.put("upload.renren.com", "upload");
    productString.put("yujian.renren.com", "yujian");
    productString.put("gift.renren.com", "gift");
    productString.put("taohua.renren.com", "taohua");
    productString.put("widget.renren.com", "widget");
    productString.put("wan.renren.com", "wan");
    productString.put("msg.renren.com", "msg");
    productString.put("safe.renren.com", "safe");
    productString.put("places.renren.com", "places");
    productString.put("j.renren.com", "j");
    productString.put("xiaozu.renren.com", "xiaozu");
    productString.put("school.renren.com", "school");
    productString.put("qun.renren.com", "qun");
    productString.put("reg.renren.com", "reg");
    productString.put("explore.renren.com", "explore");
    productString.put("xiaozhao.renren.com", "xiaozhao");
  }

  public static String identifyProduct(String host, String url) {
    if (productString.get(host) != null)
      return productString.get(host);
    for (Entry<String, Pattern> entry : productPattern.entrySet()) {
      Pattern p = entry.getValue();
      Matcher m = p.matcher(url);
      if (m.find()) {
        return entry.getKey();
      }
    }
    return "other";
  }

  public static final String SUBTOPDOMAIN = "zw,zm,za,yt,ye,ws,wf,vu,vn,vi,vg,ve,vc,va,uz,uy,us,um,uk,ug,ua,tz,tw,tv,tt,tr,to,"
      + "tn,tm,tl,tk,tj,th,tg,tf,td,tc,sz,sy,su,sv,st,sr,so,sn,sm,sl,sk,sj,si,sh,sg,se"
      + ",sd,sc,sd,sa,rw,ru,ro,rs,re,py,pw,pt,pr,pn,pm,pl,pk,ph,pg,pf,pe,pa,qa,om,nz,nu,"
      + "nt,nr,np,no,nl,ni,ng,nf,ne,nc,na,mz,my,mx,mw,mv,mu,mt,ms,mr,mq,mp,mo,mn,mm,ml,"
      + "mk,mh,mg,me,md,mc,ma,ly,lv,lu,lt,ls,lr,lk,li,lc,lb,la,kz,ky,kw,kr,kp,kn,km,ki,"
      + "kh,kg,ke,je,jp,jo,jm,it,is,ir,iq,io,in,im,il,ie,id,hu,ht,hr,hn,hm,hk,gy,gw,gu,"
      + "gt,gs,gr,gp,gn,gm,gl,gi,gh,gg,gf,ge,gd,ga,fr,fo,fm,fk,fj,fi,ev,et,es,er,eh,eg,"
      + "ee,ec,dz,do,dm,dk,dj,de,cz,cy,cx,cv,cu,cr,cq,co,cn,cm,cl,ck,ci,ch,cd,cf,cc,ca,"
      + "bz,by,bw,bv,bt,bs,br,bo,bn,bm,bj,bi,bh,bg,bf,be,bd,bb,ba,az,aw,au,at,as,ar,aq,"
      + "ao,an,am,al,ai,ag,af,ae,ad,c";
  public static final String TOPDOMAIN = "web,store,nom,firm,arts,asia,rec,post,areo,int,museum,travel,pro,mobi,info,name,biz,"
      + "mil,edu,gov,org,net,com";
  public static HashSet<String> subTopDomainSet = new HashSet<String>();
  public static HashSet<String> TopDomainSet = new HashSet<String>();

  static {
    for (int flag = 0, i = 0; i < SUBTOPDOMAIN.length(); i++) {
      if (SUBTOPDOMAIN.charAt(i) == ',') {
        subTopDomainSet.add(SUBTOPDOMAIN.substring(flag, i));
        flag = i + 1;
      } else if (i == SUBTOPDOMAIN.length() - 1) {
        subTopDomainSet.add(SUBTOPDOMAIN.substring(flag, SUBTOPDOMAIN.length()));
      }
    }
    for (int flag = 0, i = 0; i < TOPDOMAIN.length(); i++) {
      if (TOPDOMAIN.charAt(i) == ',') {
        TopDomainSet.add(TOPDOMAIN.substring(flag, i));
        flag = i + 1;
      } else if (i == TOPDOMAIN.length() - 1) {
        TopDomainSet.add(TOPDOMAIN.substring(flag, TOPDOMAIN.length()));
      }
    }
  }

  public static String identifyRefer(String referURL) {
    if (referURL == null || "".equals(referURL)) {
      return null;
    }
    String host = getHost(referURL);
    if (null != host) {
      String product = identifyProduct(host, referURL);
      if ("other" == product.toLowerCase()) {
        if (host.endsWith("renren.com")) {
          return "other.renren";
        }
        String[] domainFields = host.split("\\.");
        if (domainFields.length >= 2 && TopDomainSet.contains(domainFields[domainFields.length - 1])) {
          return domainFields[domainFields.length - 2] + "." + domainFields[domainFields.length - 1];
        }
        if (domainFields.length >= 2 && !TopDomainSet.contains(domainFields[domainFields.length - 2])
            && subTopDomainSet.contains(domainFields[domainFields.length - 1])) {
          return domainFields[domainFields.length - 2] + "." + domainFields[domainFields.length - 1];
        }
        if (domainFields.length > 2 && TopDomainSet.contains(domainFields[domainFields.length - 2])
            && subTopDomainSet.contains(domainFields[domainFields.length - 1])) {
          return domainFields[domainFields.length - 3] + "." + domainFields[domainFields.length - 2] + "."
              + domainFields[domainFields.length - 1];
        }
      } else {
        return product + ".renren";
      }
    }
    return null;
  }

  public static String getHost(String urlStr) {
    URL http;
    try {
      http = new URL(urlStr);
      return http.getHost();
    } catch (MalformedURLException e) {
    }
    return null;
  }

  public final static String dfStr = "yyyy-MM-dd HH:mm:ss";
  public final static SimpleDateFormat sdf = new SimpleDateFormat(dfStr);

  public static String getDate(long time) {
    return sdf.format(time);
  }

  // user score==user level
  public static final Integer[] scores = new Integer[] { 0, 30, 80, 150, 240, 350, 480, 560, 800, 990, 1830, 3000,
      4230, 5520, 7870, 8280, 9750, 11280, 12870, 14520, 16230, 18000, 19830, 21720, 23670, 25680, 27750, 29880, 32070,
      34320, 36630, 39000, 41430, 43920, 46470, 49080, 51750, 54480, 57270, 60120, 80000, 100000, 120000, 150000,
      200000 };

  public static int identifyUserLevel(int score) {
    score = score / 100;
    return identifyLevel(score, scores);
  }

  public static final Integer[] friendcounts = new Integer[] { 1, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000 };

  public static int identifyFriendCountLevel(int friendcount) {
    if (friendcount == 0) {
      return 0;
    } else if (friendcount > 0 && friendcount < 100) {
      return 1;
    }
    return (identifyLevel(friendcount, friendcounts) - 1) * 100;
  }

  public static int identifyLevel(int score, Integer[] scores) {
    if (score > scores[scores.length - 1])
      return scores.length;
    if (score <= 0)
      return 1;
    int min = 0, max = scores.length - 1, mid = 0;
    while (min <= max) {
      mid = (max + min) / 2;
      if (scores[mid] > score) {
        max = mid - 1;
      }
      if (scores[mid] < score) {
        min = mid + 1;
      }
      if (scores[mid] == score) {
        return mid + 1;
      }
    }
    return min;
  }

  public static int identifyContinueDays(int days) {
    // 连续登录31天以上的标识为32天
    return days > 31 ? 32 : days;
  }

  public static int identifyVip(long flag) {
    return (flag & 2) > 0 ? 1 : 0;
  }

  private static final String browserStr = "360ee,360se,chrome,firefox,maxthon,opera,qqbrowser,safari,taobao";
  private static final String ieStr = "ie,ie mobile,internet explorer";

  public static String identifyBrowser(String browserInfo) {
    String[] ies = ieStr.split(",");
    String[] browsers = browserStr.split(",");
    if (null == browserInfo || "".equals(browserInfo)) {
      return "other";
    }
    for (String ie : ies) {
      if (browserInfo.toLowerCase().contains(ie) || browserInfo.toLowerCase().startsWith(ie)) {
        return browserInfo.substring(0, browserInfo.indexOf("/"));
      }
    }
    for (String browser : browsers) {
      if (browserInfo.toLowerCase().contains(browser)) {
        return browser;
      }
    }
    return "other";
  }

  public static List<String> split(String str, String seperator) {
    List<String> fields = new ArrayList<String>();
    while (str.length() > 0) {
      int index = str.indexOf(seperator);
      String field = "";
      if (index >= 0) {
        field = str.substring(0, index);
        str = str.substring(index + seperator.length());
        fields.add(field);
      }
      if (index == -1||(index>=0&&str.length()<=0)) {
        fields.add(str);
        break;
      }
    }
    return fields;
  }
}
