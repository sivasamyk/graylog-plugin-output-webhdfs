package org.apache.hadoop.fs.http.client;

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;

/**
 * The {@link PseudoAuthenticator} implementation provides an authentication
 * equivalent to Hadoop's Simple authentication, it trusts the value of the
 * 'user.name' Java System property.
 * <p/>
 * The 'user.name' value is propagated using an additional query string
 * parameter {@link #USER_NAME} ('user.name').
 */
public class PseudoAuthenticator2 extends PseudoAuthenticator {

	private String username;

	public PseudoAuthenticator2(String username) {
		this.username = username;
	}

	@Override
	protected String getUserName() {
		return username;
	}
}
