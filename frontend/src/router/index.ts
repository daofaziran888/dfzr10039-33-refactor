import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import ScheduleExecutionView from '@/views/ScheduleExecutionView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/schedule_execution', name: 'schedule_execution', component: ScheduleExecutionView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
