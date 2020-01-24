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

class LyftOktaAuthenticationHandler
        extends AbstractHandler
{
    private static final String REDIRECT_URI = "http://localhost:5000/authorization-code/callback";

    private static final String CLIENT_ID = "0oacleef3oX94aQxj1t7";
    private static final String CLIENT_SECRET = "WEwnl6HTSZbwBak5UGuXbq1ygc7SxLxGzMONL-4k";
    private static final String BASE_URL = "https://lyft.okta.com";
    private static final String ISSUER = BASE_URL + "/oauth2/default";
    private static final String TOKEN_ENDPOINT = ISSUER + "/v1/token";
    private static final String LOGIN_ENDPOINT = ISSUER + "/v1/authorize";
    private static final String LOGIN_URL = LOGIN_ENDPOINT + "?"
            + "client_id=" + CLIENT_ID + "&"
            + "redirect_uri=" + REDIRECT_URI + "&"
            + "response_type=code&"
            + "scope=openid";

    private Server server;
    private User user;

    private static final Logger log = Logger.get(LyftOktaAuthenticationHandler.class);

    LyftOktaAuthenticationHandler(Server server, User user)
    {
        this.server = server;
        this.user = user;
    }

    @Override
    public void handle(String target,
            Request baseRequest,
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException
    {
        baseRequest.setHandled(true);

        if (target.equalsIgnoreCase("/hello")) {
            handleHello(baseRequest, response);
        }
        else if (target.equalsIgnoreCase("/authorization-code/callback")) {
            handleCallback(request, response);
        }
        else if (target.equalsIgnoreCase("/login")) {
            handleLogin(response);
        }
    }

    private void handleHello(Request baseRequest, HttpServletResponse response)
            throws IOException
    {
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        response.getWriter().println("<h1>Hello World</h1>");
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
                .add("redirect_uri", REDIRECT_URI)
                .add("client_id", CLIENT_ID)
                .add("client_secret", CLIENT_SECRET)
                .build();

        okhttp3.Request accessTokenRequest = new okhttp3.Request.Builder()
                .url(TOKEN_ENDPOINT)
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
        response.sendRedirect(LOGIN_URL);
    }
}
