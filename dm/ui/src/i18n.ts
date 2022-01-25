import i18n from 'i18next'
import { initReactI18next } from 'react-i18next'
import LanguageDetector from 'i18next-browser-languagedetector'

const translations = import.meta.globEager('../locales/*.json')

const resources = Object.keys(translations).reduce(
  (acc: Record<string, any>, path) => {
    const name = path.split('/').pop()?.replace('.json', '')
    if (name) {
      acc[name] = {
        translation: translations[path].default,
      }
    }
    return acc
  },
  {}
)

const i18nextOptions = {
  lng: JSON.parse(localStorage.getItem('lang') ?? 'null') || 'en',
  fallbackLng: 'en',
  interpolation: {
    escapeValue: false,
  },
  resources,
}

i18n.use(LanguageDetector).use(initReactI18next).init(i18nextOptions)

export default i18n
