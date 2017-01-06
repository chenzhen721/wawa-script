package tmp

import javax.net.ssl.*
import java.security.SecureRandom
import java.security.cert.X509Certificate

HttpsURLConnection.setDefaultHostnameVerifier(
        new HostnameVerifier(){
            public boolean verify(String hostname,
                                  SSLSession sslSession) {
                if (hostname.equals("test.ws.dongting.com")) {
                    return true;
                }
                return sslSession.isValid();
            }
        });

//org.apache.http.conn.ssl.SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER
TrustManager[] trustAllCerts = [new X509TrustManager(){
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s) {
    }
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s){
    }
    public X509Certificate[] getAcceptedIssuers(){return null;}
}];

// Install the all-trusting trust manager
SSLContext sc = SSLContext.getInstance("TLS");


//android -Djavax.net.debug=all
//SSLContext.getInstance("TLS","HarmonyJSSE");

sc.init(null, trustAllCerts, new SecureRandom());
HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());



//OK
println new URL("https://test.ws.dongting.com/").getText()
