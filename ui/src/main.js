import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
console.log(process.env.APP_URL);
console.log(process.env.VUE_APP_TITLE);
createApp(App).use(router).mount('#app')
