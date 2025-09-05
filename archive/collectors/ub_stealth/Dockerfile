FROM mcr.microsoft.com/playwright:v1.45.0-jammy
WORKDIR /app
RUN npm init -y >/dev/null 2>&1 && npm i redis --silent
COPY ub_stealth.js /app/ub_stealth.js
CMD ["node","/app/ub_stealth.js"]
