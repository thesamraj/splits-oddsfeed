FROM mcr.microsoft.com/playwright:v1.45.0-jammy
WORKDIR /app
# Use built-in Playwright; install only redis for publishing
RUN npm init -y >/dev/null 2>&1 && npm i redis@^4.6.13 --silent
COPY ub_dom.js /app/ub_dom.js
CMD ["node","/app/ub_dom.js"]
