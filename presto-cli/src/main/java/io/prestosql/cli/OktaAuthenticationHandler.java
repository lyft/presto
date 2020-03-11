/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.airlift.log.Logger;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Base64;

class OktaAuthenticationHandler
        extends AbstractHandler
{
    private static final String REDIRECT_URI = "http://localhost:5000/authorization-code/callback";
    private static final String STATE = "LOGIN";

    private static final String ISSUER_PATH = "/oauth2/default";
    private static final String TOKEN_PATH = "/v1/token";
    private static final String LOGIN_PATH = "/v1/authorize";

    private static final int LENGTH_CODE_VERIFIER = 64;

    private Server server;
    private User user;
    private String clientId;
    private String tokenEndpoint;
    private String loginEndpoint;

    private String codeVerifier;
    private String codeChallenge;

    private static final Logger log = Logger.get(OktaAuthenticationHandler.class);

    OktaAuthenticationHandler(String baseUrl, String clientId, Server server, User user)
    {
        this.clientId = clientId;
        this.server = server;
        this.user = user;

        String issuer = baseUrl + ISSUER_PATH;
        tokenEndpoint = issuer + TOKEN_PATH;
        loginEndpoint = issuer + LOGIN_PATH;

        SecureRandom random = new SecureRandom();
        byte[] codeVerifierBytes = new byte[LENGTH_CODE_VERIFIER];
        random.nextBytes(codeVerifierBytes);
        codeVerifier = Base64.getUrlEncoder().withoutPadding().encodeToString(codeVerifierBytes);

        // Now create code challenge
        HashFunction sha256HashFunction = Hashing.sha256();
        HashCode codeVerifierDigest = sha256HashFunction.hashBytes(codeVerifier.getBytes());
        codeChallenge = Base64.getUrlEncoder().withoutPadding().encodeToString(codeVerifierDigest.asBytes());
    }

    private String getOktaLoginUrl()
    {
        return loginEndpoint + "?"
                + "client_id=" + clientId + "&"
                + "redirect_uri=" + REDIRECT_URI + "&"
                + "response_type=code&"
                + "scope=openid&"
                + "code_challenge_method=S256&"
                + "code_challenge=" + codeChallenge + "&"
                + "state=" + STATE;
    }

    @Override
    public void handle(String target,
            Request baseRequest,
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException
    {
        baseRequest.setHandled(true);

        if (target.equalsIgnoreCase("/authorization-code/callback")) {
            handleCallback(request, response);
        }
        else if (target.equalsIgnoreCase("/login")) {
            handleLogin(response);
        }
        else {
            handle404(baseRequest, response);
        }
    }

    private void handle404(Request baseRequest, HttpServletResponse response)
            throws IOException
    {
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        baseRequest.setHandled(true);
        response.getWriter().println("<h1>Page Doesn't Exist</h1>");
    }

    private void handleCallback(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        // Read the code
        response.getWriter().println("<p>Handling callback</p>");
        String code = request.getParameter("code");
        response.getWriter().println("<p>Code: " + code + "</p>");

        // Now get the auth token
        RequestBody formBody = new FormBody.Builder()
                .add("grant_type", "authorization_code")
                .add("code", code)
                .add("code_verifier", codeVerifier)
                .add("redirect_uri", REDIRECT_URI)
                .add("client_id", clientId)
                .build();

        okhttp3.Request accessTokenRequest = new okhttp3.Request.Builder()
                .url(tokenEndpoint)
                .addHeader("User-Agent", "OkHttp Bot")
                .post(formBody)
                .build();

        OkHttpClient okHttpClient = new OkHttpClient();
        Response accessTokenResponse = okHttpClient.newCall(accessTokenRequest).execute();
        String accessTokenResponseBody = accessTokenResponse.body().string();
        response.getWriter().println("<p>Auth Response: " + accessTokenResponseBody + "</p>");

        // Parse the token
        ObjectMapper mapper = new ObjectMapper();
        JsonNode parsedJson = mapper.readTree(accessTokenResponseBody);
        String accessToken = parsedJson.get("access_token").toString();
        response.getWriter().println("<p>Access Token: " + accessToken + "</p>");

        response.getWriter().println("<a onclick='window.close();'>Close Window</a>");

        response.flushBuffer(); // Necessary to show output on the screen

        // Set the user
        user.setAccessToken(accessToken);

        // Stop the server.
        try {
            new Thread(() -> {
                try {
                    log.info("Shutting down Jetty...");
                    server.stop();
                    log.info("Jetty has stopped.");
                }
                catch (Exception ex) {
                    log.warn("Error when stopping Jetty: " + ex.getMessage());
                }
            }).start();
        }
        catch (Exception e) {
            log.warn("Cannot stop server");
            e.printStackTrace();
        }
    }

    private void handleLogin(HttpServletResponse response)
            throws IOException
    {
        response.sendRedirect(getOktaLoginUrl());
    }
}
