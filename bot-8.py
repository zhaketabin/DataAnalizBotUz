import os
import io
import logging
import sqlite3
from datetime import datetime, timedelta, date
from groq import Groq
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler
)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN",   "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
ADMIN_ID    = int(os.environ.get("ADMIN_ID", "0"))

groq_client = Groq(api_key=GROQ_API_KEY)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB = "/data/clients.db"
PROMO_END = date(2025, 5, 1)

# Conversation states
ST_LANG, ST_REGION, ST_PHONE, ST_SERVICE, ST_COMPANY = range(5)

BLOCKED_KEYWORDS = [
    "темеки", "тамаки", "tobacco", "cigarette", "сигарет",
    "алкоголь", "alcohol", "спиртной", "спирт", "пиво", "вино", "арак",
    "интим", "intim", "эротик", "sex", "секс",
    "банк", "bank", "микрокредит", "микрозайм", "кредит"
]

REGIONS_UZ = [
    ("karakalpak", "Қорақалпоғистон Республикаси"),
    ("andijan",    "Андижон вилояти"),
    ("bukhara",    "Бухоро вилояти"),
    ("jizzakh",    "Жиззах вилояти"),
    ("kashkadarya","Қашқадарё вилояти"),
    ("navoi",      "Навоий вилояти"),
    ("namangan",   "Наманган вилояти"),
    ("samarkand",  "Самарқанд вилояти"),
    ("surkhandarya","Сурхондарё вилояти"),
    ("sirdarya",   "Сирдарё вилояти"),
    ("tashkent_r", "Тошкент вилояти"),
    ("fergana",    "Фарғона вилояти"),
    ("khorezm",    "Хоразм вилояти"),
    ("tashkent_c", "Тошкент шаҳри"),
]

REGIONS = {
    "kz": {
        "karakalpak":   "Қарақалпақстан Республикасы",
        "andijan":      "Андижан облысы",
        "bukhara":      "Бұхара облысы",
        "jizzakh":      "Жиззах облысы",
        "kashkadarya":  "Қашқадарё облысы",
        "navoi":        "Навои облысы",
        "namangan":     "Наманган облысы",
        "samarkand":    "Самарқанд облысы",
        "surkhandarya": "Сурхандарё облысы",
        "sirdarya":     "Сырдарё облысы",
        "tashkent_r":   "Ташкент облысы",
        "fergana":      "Фарғона облысы",
        "khorezm":      "Хорезм облысы",
        "tashkent_c":   "Ташкент қаласы",
    },
    "uz": {
        "karakalpak":   "Қорақалпоғистон Республикаси",
        "andijan":      "Андижон вилояти",
        "bukhara":      "Бухоро вилояти",
        "jizzakh":      "Жиззах вилояти",
        "kashkadarya":  "Қашқадарё вилояти",
        "navoi":        "Навоий вилояти",
        "namangan":     "Наманган вилояти",
        "samarkand":    "Самарқанд вилояти",
        "surkhandarya": "Сурхондарё вилояти",
        "sirdarya":     "Сирдарё вилояти",
        "tashkent_r":   "Тошкент вилояти",
        "fergana":      "Фарғона вилояти",
        "khorezm":      "Хоразм вилояти",
        "tashkent_c":   "Тошкент шаҳри",
    },
    "ru": {
        "karakalpak":   "Республика Каракалпакстан",
        "andijan":      "Андижанская область",
        "bukhara":      "Бухарская область",
        "jizzakh":      "Джизакская область",
        "kashkadarya":  "Кашкадарьинская область",
        "navoi":        "Навоийская область",
        "namangan":     "Наманганская область",
        "samarkand":    "Самаркандская область",
        "surkhandarya": "Сурхандарьинская область",
        "sirdarya":     "Сырдарьинская область",
        "tashkent_r":   "Ташкентская область",
        "fergana":      "Ферганская область",
        "khorezm":      "Хорезмская область",
        "tashkent_c":   "Город Ташкент",
    },
    "en": {
        "karakalpak":   "Republic of Karakalpakstan",
        "andijan":      "Andijan Region",
        "bukhara":      "Bukhara Region",
        "jizzakh":      "Jizzakh Region",
        "kashkadarya":  "Kashkadarya Region",
        "navoi":        "Navoi Region",
        "namangan":     "Namangan Region",
        "samarkand":    "Samarkand Region",
        "surkhandarya": "Surkhandarya Region",
        "sirdarya":     "Sirdarya Region",
        "tashkent_r":   "Tashkent Region",
        "fergana":      "Fergana Region",
        "khorezm":      "Khorezm Region",
        "tashkent_c":   "Tashkent City",
    },
}

TX = {
    "kz": {
        "welcome":        "Сәлеметсіз бе! 👋\nТілді таңдаңыз:",
        "choose_region":  "📍 Облысыңызды таңдаңыз:",
        "ask_phone":      "📱 Телефон нөміріңізді жазыңыз:\nМысалы: +77001234567",
        "ask_service":    "💼 Қызмет түріңізді жазыңыз:\nМысалы: Азық-түлік дүкені",
        "blocked":        "❌ Кешіріңіз, бұл қызмет түріне рұқсат жоқ.",
        "registered":     "✅ Тіркелдіңіз!\n\n🆔 Нөміріңіз: <code>{code}</code>\n\nБұл нөмірді сақтаңыз.",
        "promo":          "🎉 1 майға дейін ТЕГІН пайдаланыңыз!\n\n",
        "not_active":     "❌ Жазылымыңыз белсенді емес.\n\n🆔 Нөміріңіз: <code>{code}</code>\n\nТөлем жасау үшін әкімшіге хабарласыңыз.",
        "choose":         "📊 Қандай бөлім керек?",
        "send_file":      "📁 Excel немесе CSV файл жіберіңіз.",
        "analyzing":      "⏳ Талдау жасалуда...",
        "file_error":     "❌ Файл оқылмады. Excel (.xlsx) немесе CSV (.csv) жіберіңіз.",
        "back":           "⬅️ Артқа",
        "ask_company":    "🏢 Компания атыңызды жазыңыз:",
        "company_saved":  "✅ Компания аты сақталды!",
        "inactive_1m":    "⚠️ Сіз 1 айдан бері ботты пайдаланбадыңыз.\n\nЕскертеміз: 2 ай толғанда профиліңіз өшіріледі.",
        "inactive_2m":    "❌ 2 ай бойы белсенді болмадыңыз.\n\nПрофиліңіз өшіріледі. Қайта тіркелу үшін /start жазыңыз.",
        "payment_ok":     "✅ Төлем қабылданды! 30 күнге рұқсат берілді.",
        "payment_warn":   "⏰ Сіз бүгін 27-күн пайдаланып жатырсыз.\n\nКелесі 30 күн үшін төлемді ұмытпаңыз!",
        "survey_q":       "📣 Хабарлама!\n\n1 майдан бастап қызмет ақылы болады — 1,000,000 сум/ай.\n\nАқылы қызметті пайдаланасыз ба?",
        "survey_yes":     "✅ Иә, пайдаланамын",
        "survey_no":      "❌ Жоқ, пайдаланбаймын",
        "survey_yes_ok":  "✅ Рахмет! Бірге жұмыс жасауды жалғастырамыз!",
        "survey_no_ok":   "❌ Түсінікті. 1 майда профиліңіз өшіріледі.",
        "sections": {
            "sotuv":    "📊 Сатылым бөлімі",
            "moliya":   "💰 Қаржы бөлімі",
            "mijozlar": "👥 Тұтынушылар бөлімі",
            "tahlil":   "📈 Талдау бөлімі",
        },
        "questions": {
            "s1": "Ең көп сатылатын тауар қайсы?",
            "s2": "Қай айда сатылым жоғары/төмен болды?",
            "s3": "Қай тұтынушы ең көп сатып алды?",
            "m1": "Жалпы пайда қанша?",
            "m2": "Шығын қай жерде көп кетеді?",
            "m3": "Өткен жылмен салыстыру",
            "j1": "Жаңа тұтынушылар саны?",
            "j2": "Кетіп қалған тұтынушылар?",
            "j3": "Ең адал тұтынушылар кімдер?",
            "t1": "Келесі айдағы сатылым болжауы",
            "t2": "Қай өнімге инвестиция салу керек?",
            "t3": "Тренд қандай?",
        }
    },
    "uz": {
        "welcome":        "Assalomu alaykum! 👋\nTilni tanlang:",
        "choose_region":  "📍 Viloyatingizni tanlang:",
        "ask_phone":      "📱 Telefon raqamingizni yozing:\nMasalan: +998901234567",
        "ask_service":    "💼 Xizmat turingizni yozing:\nMasalan: Oziq-ovqat do'koni",
        "blocked":        "❌ Kechirasiz, bu xizmat turiga ruxsat yo'q.",
        "registered":     "✅ Ro'yxatdan o'tdingiz!\n\n🆔 Raqamingiz: <code>{code}</code>\n\nBu raqamni saqlang.",
        "promo":          "🎉 1-maygacha BEPUL foydalaning!\n\n",
        "not_active":     "❌ Obunangiz faol emas.\n\n🆔 Raqamingiz: <code>{code}</code>\n\nTo'lov uchun adminga murojaat qiling.",
        "choose":         "📊 Qaysi bo'limni xohlaysiz?",
        "send_file":      "📁 Excel yoki CSV fayl yuboring.",
        "analyzing":      "⏳ Tahlil qilinmoqda...",
        "file_error":     "❌ Fayl o'qilmadi. Excel (.xlsx) yoki CSV (.csv) yuboring.",
        "back":           "⬅️ Orqaga",
        "ask_company":    "🏢 Kompaniya nomingizni yozing:",
        "company_saved":  "✅ Kompaniya nomi saqlandi!",
        "inactive_1m":    "⚠️ Siz 1 oydan beri botdan foydalanmayapsiz.\n\nEslatma: 2 oy to'lganda profilingiz o'chiriladi.",
        "inactive_2m":    "❌ 2 oy davomida faol bo'lmadingiz.\n\nProfilingiz o'chirildi. Qayta ro'yxatdan o'tish uchun /start yozing.",
        "payment_ok":     "✅ To'lov qabul qilindi! 30 kunlik ruxsat berildi.",
        "payment_warn":   "⏰ Siz bugun 27-kun foydalanmoqdasiz.\n\nKeyingi 30 kun uchun to'lovni unutmang!",
        "survey_q":       "📣 E'lon!\n\n1-maydan boshlab xizmat pullik bo'ladi — 1,000,000 so'm/oy.\n\nPullik xizmatdan foydalanasizmi?",
        "survey_yes":     "✅ Ha, foydalanaman",
        "survey_no":      "❌ Yo'q, foydalanmayman",
        "survey_yes_ok":  "✅ Rahmat! Birga ishlashda davom etamiz!",
        "survey_no_ok":   "❌ Tushunarli. 1-may kuni profilingiz o'chiriladi.",
        "sections": {
            "sotuv":    "📊 Сотув бўлими",
            "moliya":   "💰 Молия бўлими",
            "mijozlar": "👥 Мижозлар бўлими",
            "tahlil":   "📈 Таҳлил бўлими",
        },
        "questions": {
            "s1": "Энг кўп сотиладиган маҳсулот қайси?",
            "s2": "Қайси ойда сотув юқори/паст бўлди?",
            "s3": "Қайси мижоз энг кўп сотиб олди?",
            "m1": "Умумий фойда қанча?",
            "m2": "Харажат қаерда кўп кетади?",
            "m3": "Ўтган йил билан таққослаш",
            "j1": "Янги мижозлар сони?",
            "j2": "Кетиб қолган мижозлар?",
            "j3": "Содиқ мижозлар кимлар?",
            "t1": "Кейинги ойдаги сотув башорати",
            "t2": "Қайси маҳсулотга инвестиция киритиш керак?",
            "t3": "Тренд қандай?",
        }
    },
    "ru": {
        "welcome":        "Здравствуйте! 👋\nВыберите язык:",
        "choose_region":  "📍 Выберите область:",
        "ask_phone":      "📱 Введите номер телефона:\nНапример: +998901234567",
        "ask_service":    "💼 Введите вид деятельности:\nНапример: Продуктовый магазин",
        "blocked":        "❌ Извините, данный вид деятельности не допускается.",
        "registered":     "✅ Вы зарегистрированы!\n\n🆔 Ваш номер: <code>{code}</code>\n\nСохраните этот номер.",
        "promo":          "🎉 Пользуйтесь БЕСПЛАТНО до 1 мая!\n\n",
        "not_active":     "❌ Подписка неактивна.\n\n🆔 Ваш номер: <code>{code}</code>\n\nОбратитесь к администратору.",
        "choose":         "📊 Какой раздел вас интересует?",
        "send_file":      "📁 Отправьте файл Excel или CSV.",
        "analyzing":      "⏳ Анализируем...",
        "file_error":     "❌ Файл не прочитан. Отправьте Excel (.xlsx) или CSV (.csv).",
        "back":           "⬅️ Назад",
        "ask_company":    "🏢 Введите название компании:",
        "company_saved":  "✅ Название компании сохранено!",
        "inactive_1m":    "⚠️ Вы не пользовались ботом 1 месяц.\n\nНапоминаем: через 2 месяца профиль будет удалён.",
        "inactive_2m":    "❌ 2 месяца без активности.\n\nВаш профиль удалён. Напишите /start для регистрации.",
        "payment_ok":     "✅ Оплата принята! Доступ открыт на 30 дней.",
        "payment_warn":   "⏰ Сегодня 27-й день использования.\n\nНе забудьте оплатить следующие 30 дней!",
        "survey_q":       "📣 Объявление!\n\nС 1 мая сервис станет платным — 1,000,000 сум/мес.\n\nБудете пользоваться платным сервисом?",
        "survey_yes":     "✅ Да, буду",
        "survey_no":      "❌ Нет, не буду",
        "survey_yes_ok":  "✅ Спасибо! Продолжаем работать вместе!",
        "survey_no_ok":   "❌ Понятно. 1 мая ваш профиль будет удалён.",
        "sections": {
            "sotuv":    "📊 Продажи",
            "moliya":   "💰 Финансы",
            "mijozlar": "👥 Клиенты",
            "tahlil":   "📈 Аналитика",
        },
        "questions": {
            "s1": "Какой товар продаётся больше всего?",
            "s2": "В каком месяце продажи были выше/ниже?",
            "s3": "Какой клиент купил больше всего?",
            "m1": "Какова общая прибыль?",
            "m2": "Где больше всего расходов?",
            "m3": "Сравнение с прошлым годом",
            "j1": "Количество новых клиентов?",
            "j2": "Ушедшие клиенты?",
            "j3": "Кто самые лояльные клиенты?",
            "t1": "Прогноз продаж на следующий месяц",
            "t2": "В какой продукт стоит инвестировать?",
            "t3": "Какой тренд?",
        }
    },
    "en": {
        "welcome":        "Hello! 👋\nChoose your language:",
        "choose_region":  "📍 Choose your region:",
        "ask_phone":      "📱 Enter your phone number:\nExample: +998901234567",
        "ask_service":    "💼 Enter your type of business:\nExample: Grocery store",
        "blocked":        "❌ Sorry, this type of business is not allowed.",
        "registered":     "✅ You are registered!\n\n🆔 Your code: <code>{code}</code>\n\nSave this code.",
        "promo":          "🎉 Use for FREE until May 1st!\n\n",
        "not_active":     "❌ Subscription not active.\n\n🆔 Your code: <code>{code}</code>\n\nContact admin for payment.",
        "choose":         "📊 Which section do you need?",
        "send_file":      "📁 Send an Excel or CSV file.",
        "analyzing":      "⏳ Analyzing...",
        "file_error":     "❌ Could not read file. Send Excel (.xlsx) or CSV (.csv).",
        "back":           "⬅️ Back",
        "ask_company":    "🏢 Enter your company name:",
        "company_saved":  "✅ Company name saved!",
        "inactive_1m":    "⚠️ You have not used the bot for 1 month.\n\nReminder: profile will be deleted after 2 months.",
        "inactive_2m":    "❌ 2 months without activity.\n\nYour profile has been deleted. Write /start to re-register.",
        "payment_ok":     "✅ Payment accepted! Access granted for 30 days.",
        "payment_warn":   "⏰ Today is your 27th day of use.\n\nDon't forget to pay for the next 30 days!",
        "survey_q":       "📣 Announcement!\n\nFrom May 1st the service will be paid — 1,000,000 sum/month.\n\nWill you use the paid service?",
        "survey_yes":     "✅ Yes, I will",
        "survey_no":      "❌ No, I won't",
        "survey_yes_ok":  "✅ Thank you! We continue working together!",
        "survey_no_ok":   "❌ Understood. Your profile will be deleted on May 1st.",
        "sections": {
            "sotuv":    "📊 Sales",
            "moliya":   "💰 Finance",
            "mijozlar": "👥 Customers",
            "tahlil":   "📈 Analytics",
        },
        "questions": {
            "s1": "Which product sells the most?",
            "s2": "Which month had highest/lowest sales?",
            "s3": "Which customer bought the most?",
            "m1": "What is the total profit?",
            "m2": "Where are the most expenses?",
            "m3": "Comparison with last year",
            "j1": "Number of new customers?",
            "j2": "Lost customers?",
            "j3": "Who are the most loyal customers?",
            "t1": "Next month sales forecast",
            "t2": "Which product to invest in?",
            "t3": "What is the trend?",
        }
    },
}

SECTION_KEYS = {
    "sotuv":    ["s1","s2","s3"],
    "moliya":   ["m1","m2","m3"],
    "mijozlar": ["j1","j2","j3"],
    "tahlil":   ["t1","t2","t3"],
}

LANG_FULL = {"kz": "Qazaq", "uz": "O'zbek", "ru": "Russian", "en": "English"}

def t(lang, key, **kw):
    val = TX.get(lang, TX["kz"]).get(key, key)
    return val.format(**kw) if kw else val

# ─── DATABASE ────────────────────────────────────────────────────────────────
def init_db():
    os.makedirs("/data", exist_ok=True)
    with sqlite3.connect(DB) as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                telegram_id      INTEGER PRIMARY KEY,
                unique_code      TEXT UNIQUE,
                language         TEXT DEFAULT 'kz',
                region           TEXT,
                phone            TEXT,
                service          TEXT,
                company          TEXT,
                is_active        INTEGER DEFAULT 0,
                subscription_end TEXT,
                created_at       TEXT,
                last_active      TEXT,
                company_asked    INTEGER DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS blocked_phones (
                phone TEXT PRIMARY KEY,
                reason TEXT,
                blocked_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS survey (
                telegram_id INTEGER PRIMARY KEY,
                answer TEXT,
                answered_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS question_stats (
                question_key TEXT PRIMARY KEY,
                count INTEGER DEFAULT 0
            )
        """)
        c.commit()

def get_client(tid):
    with sqlite3.connect(DB) as c:
        return c.execute("SELECT * FROM clients WHERE telegram_id=?", (tid,)).fetchone()

def get_client_by_code(code):
    with sqlite3.connect(DB) as c:
        return c.execute("SELECT * FROM clients WHERE unique_code=?", (code,)).fetchone()

def is_phone_blocked(phone):
    with sqlite3.connect(DB) as c:
        return c.execute("SELECT 1 FROM blocked_phones WHERE phone=?", (phone,)).fetchone() is not None

def block_phone(phone, reason):
    with sqlite3.connect(DB) as c:
        c.execute("INSERT OR IGNORE INTO blocked_phones VALUES (?,?,?)",
                  (phone, reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        c.commit()

def create_client(tid, lang, region, phone, service):
    with sqlite3.connect(DB) as c:
        count = c.execute("SELECT COUNT(*) FROM clients").fetchone()[0] + 1
        code  = f"ZH-705{count:06d}"
        now   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT OR IGNORE INTO clients "
            "(telegram_id,unique_code,language,region,phone,service,is_active,created_at,last_active) "
            "VALUES (?,?,?,?,?,?,0,?,?)",
            (tid, code, lang, region, phone, service, now, now)
        )
        c.commit()
    return code

def set_active(tid, days=30):
    end = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
    with sqlite3.connect(DB) as c:
        c.execute("UPDATE clients SET is_active=1, subscription_end=? WHERE telegram_id=?", (end, tid))
        c.commit()
    return end

def set_active_by_code(code, days=30):
    client = get_client_by_code(code)
    if not client:
        return None
    if client[8]:
        try:
            current_end = datetime.strptime(client[8], "%Y-%m-%d")
            if current_end > datetime.now():
                end = (current_end + timedelta(days=days)).strftime("%Y-%m-%d")
            else:
                end = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
        except Exception:
            end = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
    else:
        end = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
    with sqlite3.connect(DB) as c:
        c.execute("UPDATE clients SET is_active=1, subscription_end=? WHERE unique_code=?", (end, code))
        c.commit()
    return end

def deactivate_by_code(code):
    with sqlite3.connect(DB) as c:
        c.execute("UPDATE clients SET is_active=0, subscription_end=NULL WHERE unique_code=?", (code,))
        c.commit()

def set_company(tid, company):
    with sqlite3.connect(DB) as c:
        c.execute("UPDATE clients SET company=?, company_asked=1 WHERE telegram_id=?", (company, tid))
        c.commit()

def update_last_active(tid):
    with sqlite3.connect(DB) as c:
        c.execute("UPDATE clients SET last_active=? WHERE telegram_id=?",
                  (datetime.now().strftime("%Y-%m-%d"), tid))
        c.commit()

def check_active(tid):
    cl = get_client(tid)
    if not cl or not cl[7]:
        return False
    if cl[8]:
        try:
            if datetime.now() > datetime.strptime(cl[8], "%Y-%m-%d"):
                return False
        except Exception:
            pass
    return True

def get_lang(tid):
    cl = get_client(tid)
    return cl[2] if cl else "kz"

def all_clients():
    with sqlite3.connect(DB) as c:
        return c.execute("SELECT * FROM clients").fetchall()

def auto_activate_promo(tid):
    if date.today() >= PROMO_END:
        return False
    end = PROMO_END.strftime("%Y-%m-%d")
    with sqlite3.connect(DB) as c:
        c.execute("UPDATE clients SET is_active=1, subscription_end=? WHERE telegram_id=?", (end, tid))
        c.commit()
    return True

def save_survey(tid, answer):
    with sqlite3.connect(DB) as c:
        c.execute("INSERT OR REPLACE INTO survey VALUES (?,?,?)",
                  (tid, answer, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        c.commit()

def inc_question_stat(qkey):
    with sqlite3.connect(DB) as c:
        c.execute("INSERT INTO question_stats VALUES (?,1) ON CONFLICT(question_key) DO UPDATE SET count=count+1", (qkey,))
        c.commit()

# ─── KEYBOARDS ───────────────────────────────────────────────────────────────
def lang_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🇰🇿 Қазақша",  callback_data="lang_kz"),
         InlineKeyboardButton("🇺🇿 Ўзбекча",  callback_data="lang_uz")],
        [InlineKeyboardButton("🇷🇺 Русский",   callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English",   callback_data="lang_en")],
    ])

def region_kb(lang):
    rows = []
    pair = []
    for code, _ in REGIONS_UZ:
        name = REGIONS[lang][code]
        pair.append(InlineKeyboardButton(name, callback_data=f"reg_{code}"))
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    return InlineKeyboardMarkup(rows)

def main_kb(lang):
    s = TX[lang]["sections"]
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(s["sotuv"],    callback_data="sec_sotuv")],
        [InlineKeyboardButton(s["moliya"],   callback_data="sec_moliya")],
        [InlineKeyboardButton(s["mijozlar"], callback_data="sec_mijozlar")],
        [InlineKeyboardButton(s["tahlil"],   callback_data="sec_tahlil")],
    ])

def questions_kb(lang, section):
    qs   = TX[lang]["questions"]
    keys = SECTION_KEYS[section]
    btns = [[InlineKeyboardButton(qs[k], callback_data=f"q_{k}")] for k in keys]
    btns.append([InlineKeyboardButton(t(lang, "back"), callback_data="back")])
    return InlineKeyboardMarkup(btns)

def survey_kb(lang):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(t(lang, "survey_yes"), callback_data="survey_yes"),
        InlineKeyboardButton(t(lang, "survey_no"),  callback_data="survey_no"),
    ]])

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def is_blocked_service(service):
    s = service.lower()
    return any(kw in s for kw in BLOCKED_KEYWORDS)

def days_since(date_str):
    try:
        return (datetime.now() - datetime.strptime(date_str[:10], "%Y-%m-%d")).days
    except Exception:
        return 0

# ─── PDF ─────────────────────────────────────────────────────────────────────
def generate_pdf(question, analysis, df, lang, client_code):
    buf    = io.BytesIO()
    doc    = SimpleDocTemplate(buf, pagesize=A4,
                               rightMargin=2*cm, leftMargin=2*cm,
                               topMargin=2*cm,  bottomMargin=2*cm)
    styles = getSampleStyleSheet()
    primary  = colors.HexColor("#0f2d4e")
    accent   = colors.HexColor("#2e86c1")
    light_bg = colors.HexColor("#f0f4f8")
    white    = colors.white
    dark     = colors.HexColor("#2c3e50")

    title_s = ParagraphStyle("T", parent=styles["Title"],
                              fontSize=20, textColor=white,
                              alignment=TA_CENTER, fontName="Helvetica-Bold")
    sub_s   = ParagraphStyle("S", parent=styles["Normal"],
                              fontSize=10, textColor=colors.HexColor("#bdc3c7"),
                              alignment=TA_CENTER, fontName="Helvetica")
    sec_s   = ParagraphStyle("H", parent=styles["Heading2"],
                              fontSize=12, textColor=primary,
                              spaceBefore=14, spaceAfter=6,
                              fontName="Helvetica-Bold")
    body_s  = ParagraphStyle("B", parent=styles["Normal"],
                              fontSize=10, textColor=dark,
                              leading=16, fontName="Helvetica", spaceAfter=4)
    meta_s  = ParagraphStyle("M", parent=styles["Normal"],
                              fontSize=8, textColor=colors.HexColor("#7f8c8d"),
                              alignment=TA_RIGHT, fontName="Helvetica")
    code_s  = ParagraphStyle("C", parent=styles["Normal"],
                              fontSize=11, textColor=accent,
                              alignment=TA_CENTER, fontName="Helvetica-Bold")

    story = []

    # Header
    hdr = Table([[Paragraph("DATA ANALYTICS REPORT", title_s)]], colWidths=[17*cm])
    hdr.setStyle(TableStyle([
        ("BACKGROUND",  (0,0),(-1,-1), primary),
        ("ROWPADDING",  (0,0),(-1,-1), 14),
    ]))
    story.append(hdr)
    story.append(Spacer(1, 0.2*cm))

    sub = Table([[Paragraph("DataAnalizBot — Professional Business Intelligence", sub_s)]], colWidths=[17*cm])
    sub.setStyle(TableStyle([
        ("BACKGROUND", (0,0),(-1,-1), accent),
        ("ROWPADDING", (0,0),(-1,-1), 5),
    ]))
    story.append(sub)
    story.append(Spacer(1, 0.4*cm))

    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    story.append(Paragraph(f"Generated: {now_str}  |  Language: {LANG_FULL.get(lang, lang)}", meta_s))
    story.append(Spacer(1, 0.2*cm))
    story.append(HRFlowable(width="100%", thickness=1, color=accent))
    story.append(Spacer(1, 0.3*cm))

    # Client code
    story.append(Paragraph(f"Client ID: {client_code}", code_s))
    story.append(Spacer(1, 0.3*cm))

    # Question
    story.append(Paragraph("ANALYSIS QUESTION", sec_s))
    qt = Table([[Paragraph(question, body_s)]], colWidths=[17*cm])
    qt.setStyle(TableStyle([
        ("BACKGROUND", (0,0),(-1,-1), light_bg),
        ("ROWPADDING", (0,0),(-1,-1), 10),
    ]))
    story.append(qt)
    story.append(Spacer(1, 0.3*cm))

    # Data overview
    story.append(Paragraph("DATA OVERVIEW", sec_s))
    info = [
        ["Parameter", "Value"],
        ["Columns",   ", ".join(str(c) for c in df.columns[:8])],
        ["Total Rows", str(len(df))],
        ["Date",       now_str],
    ]
    it = Table(info, colWidths=[5*cm, 12*cm])
    it.setStyle(TableStyle([
        ("BACKGROUND",   (0,0),(-1,0), primary),
        ("TEXTCOLOR",    (0,0),(-1,0), white),
        ("FONTNAME",     (0,0),(-1,0), "Helvetica-Bold"),
        ("FONTSIZE",     (0,0),(-1,-1), 9),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[white, light_bg]),
        ("GRID",         (0,0),(-1,-1), 0.5, colors.HexColor("#bdc3c7")),
        ("ROWPADDING",   (0,0),(-1,-1), 7),
        ("FONTNAME",     (0,1),(0,-1), "Helvetica-Bold"),
    ]))
    story.append(it)
    story.append(Spacer(1, 0.3*cm))

    # Sample data
    if len(df) > 0:
        story.append(Paragraph("DATA SAMPLE (First 10 Rows)", sec_s))
        cols = df.columns[:6].tolist()
        td   = [[str(c)[:18] for c in cols]]
        for _, row in df.head(10).iterrows():
            td.append([str(row[c])[:18] for c in cols])
        cw = 17*cm / len(cols)
        dt = Table(td, colWidths=[cw]*len(cols))
        dt.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,0), accent),
            ("TEXTCOLOR",     (0,0),(-1,0), white),
            ("FONTNAME",      (0,0),(-1,0), "Helvetica-Bold"),
            ("FONTSIZE",      (0,0),(-1,-1), 8),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[white, light_bg]),
            ("GRID",          (0,0),(-1,-1), 0.3, colors.HexColor("#bdc3c7")),
            ("ROWPADDING",    (0,0),(-1,-1), 5),
        ]))
        story.append(dt)
        story.append(Spacer(1, 0.3*cm))

    # AI Analysis
    story.append(HRFlowable(width="100%", thickness=1, color=accent))
    story.append(Spacer(1, 0.2*cm))
    story.append(Paragraph("AI ANALYSIS RESULTS", sec_s))

    for line in analysis.split("\n"):
        line = line.strip()
        if not line:
            story.append(Spacer(1, 0.12*cm))
            continue
        if line.startswith("**") or line.startswith("#"):
            clean = line.replace("**","").replace("#","").strip()
            story.append(Paragraph(clean, ParagraphStyle(
                "BH", parent=body_s, fontName="Helvetica-Bold", textColor=primary
            )))
        else:
            story.append(Paragraph(line, body_s))

    story.append(Spacer(1, 0.5*cm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#bdc3c7")))
    story.append(Spacer(1, 0.2*cm))
    story.append(Paragraph(
        "Confidential Business Document | DataAnalizBot",
        ParagraphStyle("F", parent=styles["Normal"], fontSize=8,
                       textColor=colors.HexColor("#95a5a6"), alignment=TA_CENTER)
    ))

    doc.build(story)
    buf.seek(0)
    return buf

# ─── REGISTRATION FLOW ───────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid = update.effective_user.id
    cl  = get_client(tid)

    if cl:
        lang = cl[2]
        if check_active(tid):
            await update.message.reply_text(t(lang, "choose"), reply_markup=main_kb(lang))
        else:
            await update.message.reply_text(t(lang, "not_active", code=cl[1]), parse_mode="HTML")
        return ConversationHandler.END

    await update.message.reply_text(
        "Сәлеметсіз бе / Assalomu alaykum / Здравствуйте / Hello! 👋\n\n"
        "Тілді таңдаңыз / Tilni tanlang / Выберите язык / Choose language:",
        reply_markup=lang_kb()
    )
    return ST_LANG

async def conv_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    await q.answer()
    lang = q.data[5:]
    context.user_data["lang"] = lang
    await q.edit_message_text(t(lang, "choose_region"), reply_markup=region_kb(lang))
    return ST_REGION

async def conv_region(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q      = update.callback_query
    await q.answer()
    lang   = context.user_data.get("lang", "kz")
    region = q.data[4:]
    context.user_data["region"] = region
    await q.edit_message_text(t(lang, "ask_phone"))
    return ST_PHONE

async def conv_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang  = context.user_data.get("lang", "kz")
    phone = update.message.text.strip()
    context.user_data["phone"] = phone

    if is_phone_blocked(phone):
        await update.message.reply_text(t(lang, "blocked"))
        return ConversationHandler.END

    await update.message.reply_text(t(lang, "ask_service"))
    return ST_SERVICE

async def conv_service(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid     = update.effective_user.id
    lang    = context.user_data.get("lang",   "kz")
    region  = context.user_data.get("region", "")
    phone   = context.user_data.get("phone",  "")
    service = update.message.text.strip()

    if is_blocked_service(service):
        block_phone(phone, service)
        await update.message.reply_text(t(lang, "blocked"))
        # Notify admin
        region_name = REGIONS.get(lang, REGIONS["kz"]).get(region, region)
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"🚫 Блокталды!\nТел: {phone}\nQызмет: {service}\nОблыс: {region_name}",
        )
        return ConversationHandler.END

    code  = create_client(tid, lang, region, phone, service)
    promo = auto_activate_promo(tid)

    promo_text  = t(lang, "promo") if promo else ""
    region_name = REGIONS.get(lang, REGIONS["kz"]).get(region, region)

    await update.message.reply_text(
        promo_text + t(lang, "registered", code=code),
        parse_mode="HTML"
    )

    if promo:
        await update.message.reply_text(t(lang, "choose"), reply_markup=main_kb(lang))
    else:
        await update.message.reply_text(t(lang, "not_active", code=code), parse_mode="HTML")

    # Notify admin
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=(
            f"🆕 Жаңа клиент!\n"
            f"🆔 Нөмір: <code>{code}</code>\n"
            f"📱 Тел: {phone}\n"
            f"💼 Қызмет: {service}\n"
            f"📍 Облыс: {region_name}\n"
            f"🌐 Тіл: {lang.upper()}\n"
            f"📅 Тіркелген: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        ),
        parse_mode="HTML"
    )

    return ConversationHandler.END

async def conv_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return ConversationHandler.END

# ─── COMPANY ASK (2 months after registration) ───────────────────────────────
async def conv_company_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid = update.effective_user.id
    if context.user_data.get("waiting_company"):
        company = update.message.text.strip()
        lang    = get_lang(tid)
        set_company(tid, company)
        context.user_data.pop("waiting_company", None)
        await update.message.reply_text(t(lang, "company_saved"))
        if check_active(tid):
            await update.message.reply_text(t(lang, "choose"), reply_markup=main_kb(lang))
        return True
    return False

# ─── CALLBACK HANDLER ────────────────────────────────────────────────────────
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    await q.answer()
    tid  = q.from_user.id
    data = q.data
    lang = get_lang(tid)

    # Survey
    if data in ("survey_yes", "survey_no"):
        answer = "yes" if data == "survey_yes" else "no"
        save_survey(tid, answer)
        await q.edit_message_text(t(lang, f"survey_{answer}_ok"))
        if answer == "yes" and check_active(tid):
            await q.message.reply_text(t(lang, "choose"), reply_markup=main_kb(lang))
        return

    if data == "back":
        await q.edit_message_text(t(lang, "choose"), reply_markup=main_kb(lang))
        return

    if data.startswith("sec_"):
        if not check_active(tid):
            cl = get_client(tid)
            await q.edit_message_text(t(lang, "not_active", code=cl[1] if cl else "—"), parse_mode="HTML")
            return
        section = data[4:]
        await q.edit_message_text(t(lang, "choose"), reply_markup=questions_kb(lang, section))
        return

    if data.startswith("q_"):
        if not check_active(tid):
            cl = get_client(tid)
            await q.edit_message_text(t(lang, "not_active", code=cl[1] if cl else "—"), parse_mode="HTML")
            return
        qkey  = data[2:]
        qtext = TX[lang]["questions"].get(qkey, qkey)
        context.user_data["question"]     = qtext
        context.user_data["question_key"] = qkey
        await q.edit_message_text(
            f"❓ <b>{qtext}</b>\n\n{t(lang, 'send_file')}",
            parse_mode="HTML"
        )
        return

# ─── FILE HANDLER ────────────────────────────────────────────────────────────
async def on_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid  = update.effective_user.id
    lang = get_lang(tid)

    # Company answer waiting
    if context.user_data.get("waiting_company"):
        await conv_company_answer(update, context)
        return

    if not check_active(tid):
        cl = get_client(tid)
        await update.message.reply_text(
            t(lang, "not_active", code=cl[1] if cl else "—"), parse_mode="HTML"
        )
        return

    question = context.user_data.get("question")
    if not question:
        await update.message.reply_text(t(lang, "choose"), reply_markup=main_kb(lang))
        return

    doc   = update.message.document
    fname = doc.file_name if doc else ""
    if not fname or not (fname.endswith(".xlsx") or fname.endswith(".xls") or fname.endswith(".csv")):
        await update.message.reply_text(t(lang, "file_error"))
        return

    wait = await update.message.reply_text(t(lang, "analyzing"))

    try:
        file = await doc.get_file()
        raw  = await file.download_as_bytearray()
        fio  = io.BytesIO(bytes(raw))
        df   = pd.read_csv(fio) if fname.endswith(".csv") else pd.read_excel(fio)

        summary = (
            f"Columns: {list(df.columns)}\n"
            f"Rows: {len(df)}\n"
            f"First 20 rows:\n{df.head(20).to_string()}\n"
            f"Statistics:\n{df.describe().to_string()}"
        )
        prompt = (
            f"You are a professional data analyst.\n"
            f"Question: {question}\n"
            f"Data:\n{summary}\n"
            f"Answer STRICTLY in {LANG_FULL.get(lang,'English')} language only. "
            f"Provide exact numbers and concise recommendations with clear structure."
        )

        models = ["llama-3.3-70b-versatile", "llama-3.1-70b-versatile", "mixtral-8x7b-32768"]
        result = None
        for m in models:
            try:
                resp   = groq_client.chat.completions.create(
                    model=m,
                    messages=[{"role":"user","content":prompt}],
                    max_tokens=2000
                )
                result = resp.choices[0].message.content
                break
            except Exception:
                continue

        if not result:
            raise Exception("All models failed")

        cl   = get_client(tid)
        code = cl[1] if cl else "—"

        await wait.delete()
        await update.message.reply_text(
            f"📊 <b>{question}</b>\n\n{result}\n\n🆔 <code>{code}</code>",
            parse_mode="HTML"
        )

        pdf_buf = generate_pdf(question, result, df, lang, code)
        await update.message.reply_document(
            document=pdf_buf,
            filename=f"DataAnaliz_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
            caption=f"📄 Professional Report | {code}"
        )

        await update.message.reply_text(t(lang, "choose"), reply_markup=main_kb(lang))
        context.user_data.pop("question",     None)
        context.user_data.pop("question_key", None)
        update_last_active(tid)

        qkey = context.user_data.get("question_key")
        if qkey:
            inc_question_stat(qkey)

    except Exception as e:
        logger.error(f"File error: {e}")
        try:
            await wait.delete()
        except Exception:
            pass
        await update.message.reply_text(t(lang, "file_error"))

# ─── TEXT HANDLER (company name input) ───────────────────────────────────────
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("waiting_company"):
        await conv_company_answer(update, context)

# ─── ADMIN COMMANDS ───────────────────────────────────────────────────────────
async def cmd_activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    args = context.args
    if not args:
        await update.message.reply_text("Ishlatish: /activate ZH-705000001\nYoki: /activate ZH-705000001 3")
        return
    code  = args[0]
    days  = int(args[1]) * 30 if len(args) > 1 else 30
    cl    = get_client_by_code(code)
    if not cl:
        await update.message.reply_text(f"❌ {code} topilmadi!")
        return
    end  = set_active_by_code(code, days)
    months = days // 30
    await update.message.reply_text(f"✅ {code} — {months} oyga faollashtirildi!\nMuddati: {end}")
    lang = cl[2]
    notify = {
        "kz": f"✅ Жазылымыңыз белсендірілді ({months} ай)!\n\nТолем жасалды, 30 күнге рұқсат берілді.",
        "uz": f"✅ Obunangiz faollashtirildi ({months} oy)!\n\nTo'lov qabul qilindi, 30 kunlik ruxsat berildi.",
        "ru": f"✅ Подписка активирована ({months} мес.)!\n\nОплата принята, доступ открыт на 30 дней.",
        "en": f"✅ Subscription activated ({months} month(s))!\n\nPayment accepted, access granted for 30 days.",
    }
    try:
        await context.bot.send_message(chat_id=cl[0], text=notify.get(lang, notify["kz"]))
        await context.bot.send_message(chat_id=cl[0], text=t(lang, "choose"), reply_markup=main_kb(lang))
    except Exception:
        pass

async def cmd_deactivate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args:
        await update.message.reply_text("Ishlatish: /deactivate ZH-705000001")
        return
    deactivate_by_code(context.args[0])
    await update.message.reply_text(f"❌ {context.args[0]} — o'chirildi!")

async def cmd_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    clients = all_clients()
    if not clients:
        await update.message.reply_text("Mijozlar yo'q.")
        return
    text = "👥 <b>Barcha mijozlar:</b>\n\n"
    for cl in clients:
        status = "✅" if cl[7] else "❌"
        text  += f"{status} <code>{cl[1]}</code> | {cl[2].upper()} | {cl[4] or '—'} | {cl[8] or '—'}\n"
    await update.message.reply_text(text, parse_mode="HTML")

async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args:
        await update.message.reply_text("Ishlatish: /broadcast Xabar matni")
        return
    msg     = " ".join(context.args)
    clients = all_clients()
    sent = failed = 0
    for cl in clients:
        if not cl[7]: continue
        try:
            await context.bot.send_message(chat_id=cl[0], text=msg)
            sent += 1
        except Exception:
            failed += 1
    await update.message.reply_text(f"✅ Yuborildi: {sent}\n❌ Yuborilmadi: {failed}")

async def cmd_survey(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    clients = all_clients()
    sent = 0
    for cl in clients:
        lang = cl[2] or "kz"
        try:
            await context.bot.send_message(
                chat_id=cl[0], text=t(lang, "survey_q"), reply_markup=survey_kb(lang)
            )
            sent += 1
        except Exception:
            pass
    await update.message.reply_text(f"✅ Opros yuborildi: {sent} ta")

async def cmd_survey_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    with sqlite3.connect(DB) as c:
        yes   = c.execute("SELECT COUNT(*) FROM survey WHERE answer='yes'").fetchone()[0]
        no    = c.execute("SELECT COUNT(*) FROM survey WHERE answer='no'").fetchone()[0]
        total = c.execute("SELECT COUNT(*) FROM survey").fetchone()[0]
    await update.message.reply_text(
        f"📊 <b>Opros natijalari:</b>\n\n✅ Ha: {yes}\n❌ Yo'q: {no}\n📝 Jami: {total}\n\n"
        f"⚠️ {no} ta foydalanuvchi 1-may kuni deaktiv bo'ladi.",
        parse_mode="HTML"
    )

async def cmd_checkinactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await _check_inactive(context)
    await update.message.reply_text("✅ Tekshirildi!")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await update.message.reply_text("""👨‍💼 <b>Admin buyruqlari:</b>

/activate ZH-705000001 — 1 oyga
/activate ZH-705000001 3 — 3 oyga
/deactivate ZH-705000001 — O'chirish
/clients — Ro'yxat
/broadcast Matn — Xabar yuborish
/survey — Opros yuborish
/surveyresults — Opros natijalari
/checkinactive — Faolsizlarni tekshirish
/adminhelp — Yordam""", parse_mode="HTML")

# ─── SCHEDULED JOBS ──────────────────────────────────────────────────────────
async def _check_inactive(context: ContextTypes.DEFAULT_TYPE):
    now     = datetime.now()
    clients = all_clients()
    for cl in clients:
        tid        = cl[0]
        code       = cl[1]
        lang       = cl[2] or "kz"
        is_active  = cl[7]
        last_active= cl[10]
        created_at = cl[9]
        company_asked = cl[11] if len(cl) > 11 else 0

        if tid == ADMIN_ID: continue

        # Ask company name after 2 months
        if not company_asked and created_at:
            if days_since(created_at) >= 60:
                try:
                    context.user_data_for = tid
                    await context.bot.send_message(chat_id=tid, text=t(lang, "ask_company"))
                except Exception:
                    pass

        if not is_active: continue

        check_date = last_active or created_at
        if not check_date: continue
        days = days_since(check_date)

        if days >= 60:
            deactivate_by_code(code)
            try:
                await context.bot.send_message(chat_id=tid, text=t(lang, "inactive_2m"))
            except Exception:
                pass
        elif days >= 30:
            try:
                await context.bot.send_message(chat_id=tid, text=t(lang, "inactive_1m"))
            except Exception:
                pass

async def _check_payment_warn(context: ContextTypes.DEFAULT_TYPE):
    clients = all_clients()
    for cl in clients:
        tid = cl[0]
        lang = cl[2] or "kz"
        sub_end = cl[8]
        if not sub_end or not cl[7]: continue
        if tid == ADMIN_ID: continue
        try:
            end_date  = datetime.strptime(sub_end, "%Y-%m-%d")
            days_left = (end_date - datetime.now()).days
            if days_left == 3:
                await context.bot.send_message(chat_id=tid, text=t(lang, "payment_warn"))
        except Exception:
            pass

async def _deactivate_survey_no(context: ContextTypes.DEFAULT_TYPE):
    if date.today() < PROMO_END: return
    with sqlite3.connect(DB) as c:
        rows = c.execute(
            "SELECT cl.telegram_id, cl.unique_code, cl.language "
            "FROM clients cl JOIN survey s ON cl.telegram_id=s.telegram_id "
            "WHERE s.answer='no' AND cl.is_active=1"
        ).fetchall()
    for tid, code, lang in rows:
        deactivate_by_code(code)
        try:
            await context.bot.send_message(chat_id=tid, text=t(lang or "kz", "inactive_2m"))
        except Exception:
            pass

async def _monthly_report(context: ContextTypes.DEFAULT_TYPE):
    now   = datetime.now()
    month = now.strftime("%Y-%m")
    clients = all_clients()

    total     = len(clients)
    active    = sum(1 for cl in clients if cl[7])
    new_month = sum(1 for cl in clients if cl[9] and cl[9].startswith(month))

    with sqlite3.connect(DB) as c:
        regions = c.execute(
            "SELECT region, COUNT(*) as cnt FROM clients GROUP BY region ORDER BY cnt DESC"
        ).fetchall()
        top_q = c.execute(
            "SELECT question_key, count FROM question_stats ORDER BY count DESC LIMIT 5"
        ).fetchall()
        services = c.execute(
            "SELECT service, COUNT(*) as cnt FROM clients GROUP BY service ORDER BY cnt DESC LIMIT 5"
        ).fetchall()

    region_text = "\n".join(
        f"  • {REGIONS['kz'].get(r,'—')}: {cnt}" for r, cnt in regions
    ) or "—"
    q_text = "\n".join(f"  • {qk}: {cnt}" for qk, cnt in top_q) or "—"
    s_text = "\n".join(f"  • {svc}: {cnt}" for svc, cnt in services) or "—"

    report = (
        f"📊 <b>Ай сайын отчёт — {month}</b>\n\n"
        f"👥 Жалпы клиенттер: {total}\n"
        f"✅ Белсенді: {active}\n"
        f"🆕 Осы ай қосылған: {new_month}\n\n"
        f"📍 Облыстар бойынша:\n{region_text}\n\n"
        f"💼 Көп қызмет түрлері:\n{s_text}\n\n"
        f"❓ Көп қойылған сұрақтар:\n{q_text}"
    )

    await context.bot.send_message(chat_id=ADMIN_ID, text=report, parse_mode="HTML")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    # Registration conversation
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            ST_LANG:    [CallbackQueryHandler(conv_lang,    pattern="^lang_")],
            ST_REGION:  [CallbackQueryHandler(conv_region,  pattern="^reg_")],
            ST_PHONE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, conv_phone)],
            ST_SERVICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, conv_service)],
        },
        fallbacks=[CommandHandler("cancel", conv_cancel)],
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("activate",      cmd_activate))
    app.add_handler(CommandHandler("deactivate",    cmd_deactivate))
    app.add_handler(CommandHandler("clients",       cmd_clients))
    app.add_handler(CommandHandler("broadcast",     cmd_broadcast))
    app.add_handler(CommandHandler("survey",        cmd_survey))
    app.add_handler(CommandHandler("surveyresults", cmd_survey_results))
    app.add_handler(CommandHandler("checkinactive", cmd_checkinactive))
    app.add_handler(CommandHandler("adminhelp",     cmd_help))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.Document.ALL, on_file))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # Scheduled jobs
    jq = app.job_queue
    jq.run_daily(_check_inactive,      datetime.strptime("10:00", "%H:%M").time())
    jq.run_daily(_check_payment_warn,  datetime.strptime("09:00", "%H:%M").time())
    jq.run_daily(_deactivate_survey_no,datetime.strptime("00:01", "%H:%M").time())
    jq.run_monthly(_monthly_report, when=datetime.strptime("08:00", "%H:%M").time(), day=1)

    logger.info("Bot started!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
