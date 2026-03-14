import { isSingBox } from '@/api'
import { GLOBAL, PROXY_TAB_TYPE } from '@/constant'
import { isHiddenGroup } from '@/helper'
import { configs } from '@/store/config'
import { proxiesTabShow, proxyGroupList, proxyMap, proxyProviederList } from '@/store/proxies'
import { rules } from '@/store/rules'
import { customGlobalNode, displayGlobalByMode, manageHiddenGroup } from '@/store/settings'
import { isEmpty } from 'lodash'
import { computed, ref } from 'vue'

const filterGroups = (all: string[]) => {
  if (manageHiddenGroup.value) {
    return all
  }

  return all.filter((name) => !isHiddenGroup(name))
}

const getRenderGroups = () => {
  if (isEmpty(proxyMap.value)) {
    return []
  }

  if (proxiesTabShow.value === PROXY_TAB_TYPE.PROVIDER) {
    return proxyProviederList.value.map((group) => group.name)
  }

  const currentGroups = getCurrentProxyGroups()

  if (proxiesTabShow.value === PROXY_TAB_TYPE.POLICY) {
    return currentGroups.filter((name) => isPolicyGroup(name))
  }

  if (proxiesTabShow.value === PROXY_TAB_TYPE.NODE) {
    return currentGroups.filter((name) => !isPolicyGroup(name))
  }

  return currentGroups
}

const getCurrentProxyGroups = () => {
  if (displayGlobalByMode.value) {
    if (configs.value?.mode.toUpperCase() === GLOBAL) {
      return [
        isSingBox.value && proxyMap.value[customGlobalNode.value] ? customGlobalNode.value : GLOBAL,
      ]
    }

    return filterGroups(proxyGroupList.value)
  }

  return filterGroups([...proxyGroupList.value, GLOBAL])
}

const ruleProxyNames = computed(() => {
  return new Set(rules.value.map((rule) => rule.proxy))
})

const referencedCurrentGroupNames = computed(() => {
  const currentGroupSet = new Set(getCurrentProxyGroups())
  const referenced = new Set<string>()

  currentGroupSet.forEach((name) => {
    const proxyGroup = proxyMap.value[name]

    proxyGroup?.all?.forEach((member) => {
      if (currentGroupSet.has(member)) {
        referenced.add(member)
      }
    })
  })

  return referenced
})

const fallbackPolicyGroupNames = computed(() => {
  const currentGroups = getCurrentProxyGroups()
  const referenced = referencedCurrentGroupNames.value

  return new Set(currentGroups.filter((name) => !referenced.has(name)))
})

const resolvedPolicyGroupNames = computed(() => {
  const currentGroupSet = new Set(getCurrentProxyGroups())
  const directRulePolicyGroups = new Set<string>()

  ruleProxyNames.value.forEach((name) => {
    if (currentGroupSet.has(name) && !referencedCurrentGroupNames.value.has(name)) {
      directRulePolicyGroups.add(name)
    }
  })

  if (directRulePolicyGroups.size > 0) {
    return directRulePolicyGroups
  }

  return fallbackPolicyGroupNames.value
})

const isPolicyGroup = (name: string) => {
  return resolvedPolicyGroupNames.value.has(name)
}

export const disableProxiesPageScroll = ref(false)
export const isProxiesPageMounted = ref(false)
export const policyGroups = computed(() => getCurrentProxyGroups().filter((name) => isPolicyGroup(name)))
export const nodeGroups = computed(() => getCurrentProxyGroups().filter((name) => !isPolicyGroup(name)))
export const renderGroups = computed(() => {
  const groups = getRenderGroups()

  if (isProxiesPageMounted.value) {
    return groups
  }

  return groups.slice(0, 16)
})
