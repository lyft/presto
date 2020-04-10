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
package io.prestosql.server;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.server.ui.ClusterResource;
import io.prestosql.server.ui.ClusterStatsResource;
import io.prestosql.server.ui.DisabledWebUiAuthenticationManager;
import io.prestosql.server.ui.UiQueryResource;
import io.prestosql.server.ui.WebUiAuthenticationManager;
import io.prestosql.server.ui.WebUiConfig;
import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.Identity;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.security.Principal;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.prestosql.server.HttpRequestSessionContext.AUTHENTICATED_IDENTITY;
import static java.util.Objects.requireNonNull;

public class WebUiModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        httpServerBinder(binder).bindResource("/ui", "webapp").withWelcomeFile("index.html");

        configBinder(binder).bindConfig(WebUiConfig.class);

        if (buildConfigObject(WebUiConfig.class).isEnabled()) {
            binder.bind(WebUiAuthenticationManager.class).to(FixedUserWebUiAuthenticationManager.class).in(Scopes.SINGLETON);
            jaxrsBinder(binder).bind(ClusterResource.class);
            jaxrsBinder(binder).bind(ClusterStatsResource.class);
            jaxrsBinder(binder).bind(UiQueryResource.class);
        }
        else {
            binder.bind(WebUiAuthenticationManager.class).to(DisabledWebUiAuthenticationManager.class).in(Scopes.SINGLETON);
        }
    }

    public static class FixedUserWebUiAuthenticationManager
            implements WebUiAuthenticationManager
    {
        @Override
        public void handleUiRequest(HttpServletRequest request, HttpServletResponse response, FilterChain nextFilter)
                throws IOException, ServletException
        {
            nextFilter.doFilter(withUsername(request, "web-ui"), response);
        }

        private static ServletRequest withUsername(HttpServletRequest request, String username)
        {
            requireNonNull(username, "username is null");
            BasicPrincipal principal = new BasicPrincipal(username);
            request.setAttribute(AUTHENTICATED_IDENTITY, Identity.forUser(username)
                    .withPrincipal(principal)
                    .build());
            return new HttpServletRequestWrapper(request)
            {
                @Override
                public Principal getUserPrincipal()
                {
                    return principal;
                }
            };
        }
    }
}
