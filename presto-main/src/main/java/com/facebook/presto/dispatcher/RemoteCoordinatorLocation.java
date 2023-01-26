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
package com.facebook.presto.dispatcher;

import com.facebook.presto.metadata.InternalNodeManager;

import javax.ws.rs.core.UriInfo;

import java.net.URI;

import static com.google.common.base.Strings.isNullOrEmpty;

public class RemoteCoordinatorLocation
        implements CoordinatorLocation
{
    private InternalNodeManager internalNodeManager;

    public RemoteCoordinatorLocation(InternalNodeManager internalNodeManager)
    {
        this.internalNodeManager = internalNodeManager;
    }

    @Override
    public URI getUri(UriInfo uriInfo, String xForwardedProto)
    {
        String scheme = isNullOrEmpty(xForwardedProto) ? uriInfo.getRequestUri().getScheme() : xForwardedProto;
        return uriInfo.getRequestUriBuilder()
                .host(internalNodeManager.getCoordinators().stream().findAny().get().getHost())
                .scheme(scheme)
                .replacePath("")
                .replaceQuery("")
                .build();
    }
}
