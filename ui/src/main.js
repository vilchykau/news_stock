import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import TradingVue from 'trading-vue-js'
console.log(process.env.APP_URL);
console.log(process.env.VUE_APP_TITLE);
createApp(App).use(router).use(TradingVue).mount('#app')
