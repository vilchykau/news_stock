<template>
    <nav class="global_nav">
        <router-link to="/" class="button_nav">News</router-link>
        <router-link to="/settings" class="button_nav">Settings</router-link>
        <router-link to="/chart" class="button_nav">Chart</router-link>
    </nav>  
</template>

<script>
export default {
    name: 'NavigationBar'
}
</script>

<style>

.global_nav{
    display: flex; 
    flex-direction: column;
}

.button_nav{
    background-color: #00000000;
    border: 0px;
    font-size: 1.6em;
    padding-top: 10px;
    color: black;
    text-decoration: none;
}

.button_nav:hover{
    color: rgb(0, 149, 255);
}

</style>
