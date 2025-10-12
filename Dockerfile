RUN apt-get update && apt-get install -y --no-install-recommends
libnss3
libnspr4
libdbus-glib-1-2
libatk1.0-0
libatk-bridge2.0-0
libcups2
libxcomposite1
libxdamage1
libxfixes3
libxrandr2
libgbm1
libpango-1.0-0
libcairo2
libasound2
libatspi2.0-0
&& rm -rf /var/lib/apt/lists/*