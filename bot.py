import os
import logging
import sqlite3
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler
)
import google.generativeai as genai
import pandas as pd
import io

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "YOUR_GEMINI_API_KEY")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))  # Your Telegram user ID

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── STATES ──────────────────────────────────────────────────────────────────
LANGUAGE, WAITING_FILE = range(2)

# ─── DATABASE ────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect("clients.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            telegram_id INTEGER PRIMARY KEY,
            unique_code TEXT UNIQUE,
            language TEXT DEFAULT 'uz',
            is_active INTEGER DEFAULT 0,
            subscription_end TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def get_client(telegram_id):
    conn = sqlite3.connect("clients.db")
    c = conn.cursor()
    c.execute("SELECT * FROM clients WHERE telegram_id=?", (telegram_id,))
    row = c.fetchone()
    conn.close()
    return row

def create_client(telegram_id, language):
    conn = sqlite3.connect("clients.db")
    c = conn.cursor()
    # Generate unique code ZH-705XXXXXXX
    c.execute("SELECT COUNT(*) FROM clients")
    count = c.fetchone()[0] + 1
    unique_code = f"ZH-705{count:06d}"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
        INSERT OR IGNORE INTO clients (telegram_id, unique_code, language, is_active, created_at)
        VALUES (?, ?, ?, 0, ?)
    """, (telegram_id, unique_code, language, now))
    conn.commit()
    conn.close()
    return unique_code

def update_language(telegram_id, lang):
    conn = sqlite3.connect("clients.db")
    c = conn.cursor()
    c.execute("UPDATE clients SET language=? WHERE telegram_id=?", (lang, telegram_id))
    conn.commit()
    conn.close()

def set_active(unique_code, active: bool, months=1):
    conn = sqlite3.connect("clients.db")
    c = conn.cursor()
    from datetime import timedelta
    end_date = (datetime.now() + timedelta(days=30 * months)).strftime("%Y-%m-%d")
    c.execute("""
        UPDATE clients SET is_active=?, subscription_end=? WHERE unique_code=?
    """, (1 if active else 0, end_date if active else None, unique_code))
    conn.commit()
    conn.close()

def is_active(telegram_id):
    conn = sqlite3.connect("clients.db")
    c = conn.cursor()
    c.execute("SELECT is_active, subscription_end FROM clients WHERE telegram_id=?", (telegram_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return False
    if not row[0]:
        return False
    if row[1]:
        end = datetime.strptime(row[1], "%Y-%m-%d")
        if datetime.now() > end:
            return False
    return True

def get_all_clients():
    conn = sqlite3.connect("clients.db")
    c = conn.cursor()
    c.execute("SELECT telegram_id, unique_code, language, is_active, subscription_end, created_at FROM clients")
    rows = c.fetchall()
    conn.close()
    return rows

# ─── TEXTS ───────────────────────────────────────────────────────────────────
TEXTS = {
    "uz": {
        "welcome": "Assalomu alaykum! 👋\nMen — Data Analiz Botiman.\n\nIltimos, tilni tanlang:",
        "choose_lang": "Tilni tanlang / Выберите язык:",
        "your_code": "✅ Ro'yxatdan o'tdingiz!\n\n🆔 Sizning unikal raqamingiz:\n<b>{code}</b>\n\nUshbu raqamni saqlab qo'ying — to'lovni shu raqam orqali tekshiramiz.",
        "not_active": "❌ Sizning obunangiz faol emas.\n\n🆔 Sizning raqamingiz: <b>{code}</b>\n\nTo'lov qilish uchun adminga murojaat qiling.",
        "choose_question": "📊 Qaysi tahlilni xohlaysiz?",
        "send_file": "📁 Iltimos, Excel yoki CSV fayl yuboring.",
        "analyzing": "⏳ Tahlil qilinmoqda, biroz kuting...",
        "error_file": "❌ Faylni o'qishda xatolik. Iltimos, Excel (.xlsx) yoki CSV (.csv) fayl yuboring.",
        "menu": "🏠 Bosh menyu",
        "back": "⬅️ Orqaga",
        "sections": {
            "sotuv": "📊 Сотув бўлими",
            "moliya": "💰 Молия бўлими",
            "mijozlar": "👥 Мижозлар бўлими",
            "tahlil": "📈 Таҳлил бўлими",
        },
        "questions": {
            "sotuv_1": "Энг кўп сотиладиган маҳсулот қайси?",
            "sotuv_2": "Қайси ойда сотув юқори/паст бўлди?",
            "sotuv_3": "Қайси мижоз энг кўп сотиб олди?",
            "moliya_1": "Умумий фойда қанча?",
            "moliya_2": "Харажат қаерда кўп кетади?",
            "moliya_3": "Ўтган йил билан таққослаш",
            "mijozlar_1": "Янги мижозлар сони?",
            "mijozlar_2": "Кетиб қолган мижозлар?",
            "mijozlar_3": "Содиқ мижозлар кимлар?",
            "tahlil_1": "Кейинги ойдаги сотув таҳлили",
            "tahlil_2": "Қайси маҳсулотга инвестиция киритиш керак?",
            "tahlil_3": "Тренд қандай?",
        }
    },
    "kz": {
        "welcome": "Сәлеметсіз бе! 👋\nМен — Дата Аналитика Ботымын.\n\nТілді таңдаңыз:",
        "choose_lang": "Тілді таңдаңыз:",
        "your_code": "✅ Тіркелдіңіз!\n\n🆔 Сіздің уникал нөміріңіз:\n<b>{code}</b>\n\nБұл нөмірді сақтаңыз — төлемді осы нөмір арқылы тексереміз.",
        "not_active": "❌ Сіздің жазылымыңыз белсенді емес.\n\n🆔 Сіздің нөміріңіз: <b>{code}</b>\n\nТөлем жасау үшін әкімшіге хабарласыңыз.",
        "choose_question": "📊 Қандай талдау керек?",
        "send_file": "📁 Excel немесе CSV файл жіберіңіз.",
        "analyzing": "⏳ Талдау жасалуда, күте тұрыңыз...",
        "error_file": "❌ Файлды оқуда қате. Excel (.xlsx) немесе CSV (.csv) файл жіберіңіз.",
        "menu": "🏠 Басты мәзір",
        "back": "⬅️ Артқа",
        "sections": {
            "sotuv": "📊 Сатылым бөлімі",
            "moliya": "💰 Қаржы бөлімі",
            "mijozlar": "👥 Тұтынушылар бөлімі",
            "tahlil": "📈 Талдау бөлімі",
        },
        "questions": {
            "sotuv_1": "Ең көп сатылатын тауар қайсы?",
            "sotuv_2": "Қай айда сатылым жоғары/төмен болды?",
            "sotuv_3": "Қай тұтынушы ең көп сатып алды?",
            "moliya_1": "Жалпы пайда қанша?",
            "moliya_2": "Шығын қай жерде көп кетеді?",
            "moliya_3": "Өткен жылмен салыстыру",
            "mijozlar_1": "Жаңа тұтынушылар саны?",
            "mijozlar_2": "Кетіп қалған тұтынушылар?",
            "mijozlar_3": "Ең адал тұтынушылар кімдер?",
            "tahlil_1": "Келесі айдағы сатылым талдауы",
            "tahlil_2": "Қай өнімге инвестиция салу керек?",
            "tahlil_3": "Тренд қандай?",
        }
    },
    "ru": {
        "welcome": "Здравствуйте! 👋\nЯ — Бот Аналитики Данных.\n\nПожалуйста, выберите язык:",
        "choose_lang": "Выберите язык:",
        "your_code": "✅ Вы зарегистрированы!\n\n🆔 Ваш уникальный номер:\n<b>{code}</b>\n\nСохраните этот номер — оплата проверяется по нему.",
        "not_active": "❌ Ваша подписка неактивна.\n\n🆔 Ваш номер: <b>{code}</b>\n\nДля оплаты обратитесь к администратору.",
        "choose_question": "📊 Какой анализ вы хотите?",
        "send_file": "📁 Пожалуйста, отправьте файл Excel или CSV.",
        "analyzing": "⏳ Анализируем, подождите...",
        "error_file": "❌ Ошибка при чтении файла. Отправьте Excel (.xlsx) или CSV (.csv).",
        "menu": "🏠 Главное меню",
        "back": "⬅️ Назад",
        "sections": {
            "sotuv": "📊 Отдел продаж",
            "moliya": "💰 Финансовый отдел",
            "mijozlar": "👥 Отдел клиентов",
            "tahlil": "📈 Отдел аналитики",
        },
        "questions": {
            "sotuv_1": "Какой товар продаётся больше всего?",
            "sotuv_2": "В каком месяце продажи были высокими/низкими?",
            "sotuv_3": "Какой клиент купил больше всего?",
            "moliya_1": "Какова общая прибыль?",
            "moliya_2": "Где больше всего расходов?",
            "moliya_3": "Сравнение с прошлым годом",
            "mijozlar_1": "Количество новых клиентов?",
            "mijozlar_2": "Ушедшие клиенты?",
            "mijozlar_3": "Кто самые лояльные клиенты?",
            "tahlil_1": "Прогноз продаж на следующий месяц",
            "tahlil_2": "В какой продукт стоит инвестировать?",
            "tahlil_3": "Какой тренд?",
        }
    },
    "en": {
        "welcome": "Hello! 👋\nI am a Data Analytics Bot.\n\nPlease choose your language:",
        "choose_lang": "Choose language:",
        "your_code": "✅ You are registered!\n\n🆔 Your unique code:\n<b>{code}</b>\n\nSave this code — payments are verified by it.",
        "not_active": "❌ Your subscription is not active.\n\n🆔 Your code: <b>{code}</b>\n\nContact admin to make payment.",
        "choose_question": "📊 What analysis do you need?",
        "send_file": "📁 Please send an Excel or CSV file.",
        "analyzing": "⏳ Analyzing, please wait...",
        "error_file": "❌ Error reading file. Please send Excel (.xlsx) or CSV (.csv).",
        "menu": "🏠 Main menu",
        "back": "⬅️ Back",
        "sections": {
            "sotuv": "📊 Sales Department",
            "moliya": "💰 Finance Department",
            "mijozlar": "👥 Customers Department",
            "tahlil": "📈 Analytics Department",
        },
        "questions": {
            "sotuv_1": "Which product sells the most?",
            "sotuv_2": "Which month had highest/lowest sales?",
            "sotuv_3": "Which customer bought the most?",
            "moliya_1": "What is the total profit?",
            "moliya_2": "Where are the most expenses?",
            "moliya_3": "Comparison with last year",
            "mijozlar_1": "Number of new customers?",
            "mijozlar_2": "Lost customers?",
            "mijozlar_3": "Who are the most loyal customers?",
            "tahlil_1": "Next month sales forecast",
            "tahlil_2": "Which product to invest in?",
            "tahlil_3": "What is the trend?",
        }
    }
}

def t(lang, key, **kwargs):
    text = TEXTS.get(lang, TEXTS["uz"]).get(key, key)
    if kwargs:
        text = text.format(**kwargs)
    return text

def get_lang(telegram_id):
    client = get_client(telegram_id)
    if client:
        return client[2] or "uz"
    return "uz"

# ─── KEYBOARDS ───────────────────────────────────────────────────────────────
def lang_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🇺🇿 Ўзбекча", callback_data="lang_uz"),
         InlineKeyboardButton("🇰🇿 Қазақша", callback_data="lang_kz")],
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru"),
         InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
    ])

def main_menu_keyboard(lang):
    tx = TEXTS.get(lang, TEXTS["uz"])
    sections = tx["sections"]
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(sections["sotuv"], callback_data="section_sotuv")],
        [InlineKeyboardButton(sections["moliya"], callback_data="section_moliya")],
        [InlineKeyboardButton(sections["mijozlar"], callback_data="section_mijozlar")],
        [InlineKeyboardButton(sections["tahlil"], callback_data="section_tahlil")],
    ])

def questions_keyboard(lang, section):
    tx = TEXTS.get(lang, TEXTS["uz"])
    qs = tx["questions"]
    keys = [k for k in qs if k.startswith(section)]
    buttons = [[InlineKeyboardButton(qs[k], callback_data=f"q_{k}")] for k in keys]
    buttons.append([InlineKeyboardButton(t(lang, "back"), callback_data="back_menu")])
    return InlineKeyboardMarkup(buttons)

# ─── HANDLERS ────────────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    client = get_client(user_id)
    if not client:
        await update.message.reply_text(
            TEXTS["uz"]["welcome"],
            reply_markup=lang_keyboard()
        )
    else:
        lang = client[2]
        if is_active(user_id):
            await update.message.reply_text(
                t(lang, "choose_question"),
                reply_markup=main_menu_keyboard(lang)
            )
        else:
            await update.message.reply_text(
                t(lang, "not_active", code=client[1]),
                parse_mode="HTML"
            )

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    # Language selection
    if data.startswith("lang_"):
        lang = data.split("_")[1]
        client = get_client(user_id)
        if not client:
            code = create_client(user_id, lang)
            await query.edit_message_text(
                t(lang, "your_code", code=code),
                parse_mode="HTML"
            )
            # Show not active message after registration
            await query.message.reply_text(
                t(lang, "not_active", code=code),
                parse_mode="HTML"
            )
        else:
            update_language(user_id, lang)
            if is_active(user_id):
                await query.edit_message_text(
                    t(lang, "choose_question"),
                    reply_markup=main_menu_keyboard(lang)
                )
            else:
                await query.edit_message_text(
                    t(lang, "not_active", code=get_client(user_id)[1]),
                    parse_mode="HTML"
                )
        return

    lang = get_lang(user_id)

    # Check subscription
    if not is_active(user_id):
        client = get_client(user_id)
        code = client[1] if client else "—"
        await query.edit_message_text(
            t(lang, "not_active", code=code),
            parse_mode="HTML"
        )
        return

    # Back to menu
    if data == "back_menu":
        await query.edit_message_text(
            t(lang, "choose_question"),
            reply_markup=main_menu_keyboard(lang)
        )
        return

    # Section selected
    if data.startswith("section_"):
        section = data.split("_")[1]
        await query.edit_message_text(
            t(lang, "choose_question"),
            reply_markup=questions_keyboard(lang, section)
        )
        return

    # Question selected
    if data.startswith("q_"):
        question_key = data[2:]
        tx = TEXTS.get(lang, TEXTS["uz"])
        question_text = tx["questions"].get(question_key, question_key)
        context.user_data["pending_question"] = question_text
        context.user_data["pending_lang"] = lang
        await query.edit_message_text(
            f"❓ <b>{question_text}</b>\n\n{t(lang, 'send_file')}",
            parse_mode="HTML"
        )
        return

async def file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    lang = get_lang(user_id)

    if not is_active(user_id):
        client = get_client(user_id)
        code = client[1] if client else "—"
        await update.message.reply_text(
            t(lang, "not_active", code=code),
            parse_mode="HTML"
        )
        return

    question = context.user_data.get("pending_question")
    if not question:
        await update.message.reply_text(t(lang, "choose_question"), reply_markup=main_menu_keyboard(lang))
        return

    doc = update.message.document
    if not doc:
        await update.message.reply_text(t(lang, "error_file"))
        return

    file_name = doc.file_name or ""
    if not (file_name.endswith(".xlsx") or file_name.endswith(".xls") or file_name.endswith(".csv")):
        await update.message.reply_text(t(lang, "error_file"))
        return

    wait_msg = await update.message.reply_text(t(lang, "analyzing"))

    try:
        file = await doc.get_file()
        file_bytes = await file.download_as_bytearray()
        file_io = io.BytesIO(bytes(file_bytes))

        if file_name.endswith(".csv"):
            df = pd.read_csv(file_io)
        else:
            df = pd.read_excel(file_io)

        # Prepare data summary for Gemini
        data_summary = f"""
Fayl: {file_name}
Ustunlar: {list(df.columns)}
Qatorlar soni: {len(df)}
Birinchi 20 qator:
{df.head(20).to_string()}

Statistika:
{df.describe().to_string()}
"""

        prompt = f"""
Sen professional data analitiksan. Quyidagi ma'lumotlarga asoslanib savolga javob ber.

Savol: {question}

Ma'lumotlar:
{data_summary}

Iltimos:
1. Aniq va tushunarli javob ber
2. Muhim raqamlarni ko'rsat
3. Qisqa tavsiyalar ber
4. Javobni {lang} tilida yoz (uz=o'zbek, kz=qozoq, ru=rus, en=ingliz)
"""

        response = model.generate_content(prompt)
        result_text = response.text

        await wait_msg.delete()
        await update.message.reply_text(
            f"📊 <b>{question}</b>\n\n{result_text}",
            parse_mode="HTML"
        )
        await update.message.reply_text(
            t(lang, "choose_question"),
            reply_markup=main_menu_keyboard(lang)
        )
        context.user_data.pop("pending_question", None)

    except Exception as e:
        logger.error(f"Error: {e}")
        await wait_msg.delete()
        await update.message.reply_text(t(lang, "error_file"))

# ─── ADMIN COMMANDS ───────────────────────────────────────────────────────────
async def admin_activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    args = context.args
    if len(args) < 1:
        await update.message.reply_text("Ishlatish: /activate ZH-705000001")
        return
    code = args[0]
    months = int(args[1]) if len(args) > 1 else 1
    set_active(code, True, months)
    await update.message.reply_text(f"✅ {code} — {months} oyga faollashtirildi!")

async def admin_deactivate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    args = context.args
    if len(args) < 1:
        await update.message.reply_text("Ishlatish: /deactivate ZH-705000001")
        return
    code = args[0]
    set_active(code, False)
    await update.message.reply_text(f"❌ {code} — o'chirildi!")

async def admin_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    clients = get_all_clients()
    if not clients:
        await update.message.reply_text("Hech qanday mijoz yo'q.")
        return
    text = "👥 <b>Barcha mijozlar:</b>\n\n"
    for c in clients:
        status = "✅ Faol" if c[3] else "❌ Faol emas"
        end = c[4] or "—"
        text += f"🆔 <b>{c[1]}</b> | {status} | {end}\n"
    await update.message.reply_text(text, parse_mode="HTML")

async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    text = """
👨‍💼 <b>Admin buyruqlari:</b>

/activate ZH-705000001 — Faollashtirish (1 oy)
/activate ZH-705000001 3 — 3 oyga faollashtirish
/deactivate ZH-705000001 — O'chirish
/clients — Barcha mijozlar ro'yxati
/adminhelp — Yordam
"""
    await update.message.reply_text(text, parse_mode="HTML")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("activate", admin_activate))
    app.add_handler(CommandHandler("deactivate", admin_deactivate))
    app.add_handler(CommandHandler("clients", admin_clients))
    app.add_handler(CommandHandler("adminhelp", admin_help))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, file_handler))

    logger.info("Bot started!")
    app.run_polling()

if __name__ == "__main__":
    main()
