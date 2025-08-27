FROM mcr.microsoft.com/playwright:v1.55.0-jammy
WORKDIR /app
RUN npm init -y && npm install playwright ioredis express
COPY pusher.js /app/pusher.js
ENV PORT=9118
CMD ["node","/app/pusher.js"]
