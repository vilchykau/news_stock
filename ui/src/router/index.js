import {createRouter, createWebHistory} from 'vue-router';
import NewsPage from '../views/NewsPage.vue'
import SettingsPage from '../views/SettingsPage.vue'

const routes = [
    {
        path: '/',
        name: 'Home',
        component: NewsPage
    },
    {
        path: '/settings',
        name: 'Settings',
        component: SettingsPage
    }
];

const router = createRouter({
    history: createWebHistory(),
    routes,
    mode: 'history'
});

export default router;