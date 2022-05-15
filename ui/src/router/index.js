import {createRouter, createWebHistory} from 'vue-router';
import NewsPage from '../views/NewsPage.vue'
import SettingsPage from '../views/SettingsPage.vue'
import ChartPage from '../views/ChartPage.vue'

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
    },
    {
        path: '/chart',
        name: 'Chart',
        component: ChartPage
    }
];

const router = createRouter({
    history: createWebHistory(),
    routes,
    mode: 'history'
});

export default router;