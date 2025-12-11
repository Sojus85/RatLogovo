# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import random
from collections import Counter
from sqlalchemy import create_engine, text
from config import DB_NAME, TAG_MAPPING
import re

# --- ВАЖНО: ИСПОЛЬЗУЕМ PYMORPHY3 ДЛЯ СОВМЕСТИМОСТИ ---
try:
    import pymorphy3 as pymorphy2
except ImportError:
    import pymorphy2

# --- ПАТЧ ДЛЯ PYTHON 3.13 ---
import inspect
if not hasattr(inspect, 'getargspec'):
    def getargspec_stub(func):
        spec = inspect.getfullargspec(func)
        return (spec.args, spec.varargs, spec.varkw, spec.defaults)
    inspect.getargspec = getargspec_stub

# --- СЛОВАРИ ---
STOPWORDS = set([
    'и', 'в', 'во', 'не', 'что', 'он', 'на', 'я', 'с', 'со', 'как', 'а', 'то', 'все', 'она', 'так', 'его', 'но', 'да', 'ты', 'к', 'у', 'же', 'вы', 'за', 'бы', 'по', 'только', 'ее', 'мне', 'было', 'вот', 'от', 'меня', 'еще', 'нет', 'о', 'из', 'ему', 'теперь', 'когда', 'даже', 'ну', 'вдруг', 'ли', 'если', 'уже', 'или', 'ни', 'быть', 'был', 'него', 'до', 'вас', 'нибудь', 'опять', 'уж', 'вам', 'ведь', 'там', 'потом', 'себя', 'ничего', 'ей', 'может', 'они', 'тут', 'где', 'есть', 'надо', 'ней', 'для', 'мы', 'тебя', 'их', 'чем', 'была', 'сам', 'чтоб', 'без', 'будто', 'чего', 'раз', 'тоже', 'себе', 'под', 'будет', 'ж', 'тогда', 'кто', 'этот', 'того', 'потому', 'этого', 'какой', 'совсем', 'ним', 'здесь', 'этом', 'один', 'почти', 'мой', 'тем', 'чтобы', 'нее', 'сейчас', 'были', 'куда', 'зачем', 'всех', 'никогда', 'можно', 'при', 'наконец', 'два', 'об', 'другой', 'хоть', 'после', 'над', 'больше', 'тот', 'через', 'эти', 'нас', 'про', 'всего', 'них', 'какая', 'много', 'разве', 'три', 'эту', 'моя', 'впрочем', 'хорошо', 'свою', 'этой', 'перед', 'иногда', 'лучше', 'чуть', 'том', 'нельзя', 'такой', 'им', 'более', 'всегда', 'конечно', 'всю', 'между', 'это', 'всё', 'ещё', 'просто', 'мочь', 'который', 'весь', 'свой', 'твой', 'наш', 'ваш', 'самый', 'очень', 'вообще', 'нужно', 'сказать', 'говорить', 'думать', 'хотеть', 'знать', 'сча', 'ща', 'кста', 'типа', 'короче', 'лан', 'пох', 'норм', 'ок', 'пока', 'привет', 'сделать', 'делать', 'пойти', 'идти', 'видеть', 'смотреть', 'дать', 'понимать', 'понять', 'стать', 'ждать', 'взять', 'написать', 'писать', 'спросить', 'помнить', 'любить', 'смочь', 'хотеться', 'иметь', 'сидеть', 'стоять', 'лежать', 'выйти', 'играть', 'игра', 'время', 'день', 'год', 'человек',
    'подписаться', 'канал', 'ссылка', 'комментарий', 'пост', 'реклама', 'источник', 'читать', 'новости', 'телеграч', 'tme', 'http', 'https', 'com', 'ru', 'www', 'html', 'bot', 'via', 'произойти', 'сообщать', 'данные', 'версия', 'сайт', 'главный', 'страница', 'вернуться', 'домой', 'кнопка', 'надпись', 'перевести', 'ошибка', 'открывать', 'удалить'
])

BAD_ROOTS = ['хуй', 'хуе', 'хуё', 'бля', 'пизд', 'еба', 'еб', 'хер', 'говн', 'чмо', 'муд', 'срат', 'жоп', 'лох', 'сук', 'еблан', 'гандон', 'пидор', 'даун', 'мраз', 'тварь']

st.set_page_config(page_title="VibeCheck Analytics", page_icon="📊", layout="wide")

@st.cache_resource
def get_db_engine():
    return create_engine(f'sqlite:///{DB_NAME}')

@st.cache_resource
def get_morph():
    return pymorphy2.MorphAnalyzer()

engine = get_db_engine()
morph = get_morph()

def format_duration(seconds):
    if not seconds: return "0 сек"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0: return f"{h}ч {m}м"
    if m > 0: return f"{m}м {s}с"
    return f"{s} сек"

def load_data_from_db(date_range=None):
    query = "SELECT * FROM messages"
    params = {}
    if date_range and len(date_range) == 2:
        query += " WHERE date >= :start AND date <= :end"
        params = {"start": date_range[0], "end": date_range[1]}
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)
        if df.empty: return df

        BANNED_NAMES = ["Иван Ежик", "Привалов", "GigaChat"] 
        df = df[~df['username'].isin(BANNED_NAMES)]

        df['date'] = pd.to_datetime(df['date'])
        df['hour'] = df['date'].dt.hour
        df['day_name'] = df['date'].dt.day_name()
        df['day_idx'] = df['date'].dt.weekday
        
        def count_bad(txt):
            if not txt: return 0
            txt = txt.lower()
            return sum(1 for root in BAD_ROOTS if root in txt)
            
        df['bad_count'] = df['text'].apply(count_bad)
        return df
    except Exception as e:
        st.error(f"Ошибка: {e}")
        return pd.DataFrame()

def load_mentions(date_range=None):
    query = "SELECT * FROM mentions"
    params = {}
    if date_range and len(date_range) == 2:
        query += " WHERE date >= :start AND date <= :end"
        params = {"start": date_range[0], "end": date_range[1]}
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)
        return df
    except:
        return pd.DataFrame()

# --- ТОП СЛОВ ---
@st.cache_data
def get_top_words(df, username):
    user_msgs = df[(df['username'] == username) & (df['is_forwarded'] == False)]
    text_data = " ".join(user_msgs['text'].dropna())
    text_data = re.sub(r'@\w+', '', text_data)
    text_data = re.sub(r'http\S+', '', text_data)
    text_data = re.sub(r'[^а-яА-ЯёЁa-zA-Z\s]', '', text_data).lower()
    
    words = text_data.split()
    lemmas = []
    for w in words:
        if len(w) > 2 and w not in STOPWORDS:
            normal_form = morph.parse(w)[0].normal_form
            if normal_form not in STOPWORDS:
                lemmas.append(normal_form)
    
    return Counter(lemmas).most_common(10)

# --- MAIN ---
def main():
    st.title("📊 VibeCheck: Итоги")
    
    with engine.connect() as conn:
        min_date = conn.execute(text("SELECT min(date) FROM messages")).scalar()
        max_date = conn.execute(text("SELECT max(date) FROM messages")).scalar()
    
    if not min_date:
        st.error("База пуста.")
        return

    min_date = pd.to_datetime(min_date).date()
    max_date = pd.to_datetime(max_date).date()
    date_range = st.sidebar.date_input("Период", [min_date, max_date], min_value=min_date, max_value=max_date)
    
    if st.sidebar.button("🔄 Обновить"):
        st.cache_data.clear()
        st.rerun()

    df = load_data_from_db(date_range)
    df_mentions = load_mentions(date_range)
    
    if df.empty: return

    df['voice_duration'] = df.apply(lambda x: x['duration'] if x['media_type'] == 'voice' else 0, axis=1)
    df['video_duration'] = df.apply(lambda x: x['duration'] if x['media_type'] == 'video_note' else 0, axis=1)
    df_clean = df[df['is_forwarded'] == False]

    tab_summary, tab_hall, tab_psycho, tab_mentions, tab_words, tab_game, tab_search = st.tabs([
        "📈 Сводка", "🏆 Зал Славы", "🧠 Рейтинг", "🔗 Связи", "🗣️ Лексика", "🎮 Битва", "🕵️ Поиск"
    ])

    # --- 1. СВОДКА ---
    with tab_summary:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Всего сообщений", len(df))
        c2.metric("Дней переписки", df['date'].dt.date.nunique())
        c3.metric("Картинок", len(df[df['media_type'] == 'photo']))
        c4.metric("Самый активный", df['username'].value_counts().idxmax())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Голосовых (время)", format_duration(df['voice_duration'].sum()))
        c2.metric("Кружочков (время)", format_duration(df['video_duration'].sum()))
        c3.metric("Видео файлов", len(df[df['media_type'] == 'video_file']))
        c4.metric("Стикеров", len(df[df['media_type'] == 'sticker']))

        st.divider()
        st.subheader("Динамика общения")
        timeline = df.groupby([pd.Grouper(key='date', freq='D'), 'username']).size().reset_index(name='count')
        st.plotly_chart(px.line(timeline, x='date', y='count', color='username', template="plotly_dark"), use_container_width=True)

        st.subheader("Тепловая карта активности")
        heatmap = df.groupby(['day_name', 'day_idx', 'hour']).size().reset_index(name='count')
        heatmap = heatmap.sort_values(['day_idx', 'hour'])
        fig_heat = px.density_heatmap(heatmap, x='hour', y='day_name', z='count', nbinsx=24, color_continuous_scale='Viridis', template="plotly_dark")
        fig_heat.update_layout(yaxis={'categoryorder':'array', 'categoryarray': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']})
        st.plotly_chart(fig_heat, use_container_width=True)

    # --- 2. ЗАЛ СЛАВЫ ---
    with tab_hall:
        st.header("🏆 Зал Славы")
        st.caption("Учитываются только оригинальные сообщения (без репостов), кроме лайков.")
        
        def leaderboard(title, icon, col, agg_func='sum', suffix="", is_duration=False, use_full_df=False):
            target_df = df if use_full_df else df_clean
            res = target_df.groupby('username')[col].agg(agg_func).sort_values(ascending=False)
            if res.empty: return
            
            winner = res.index[0]
            val = res.iloc[0]
            val_str = format_duration(val) if is_duration else f"{int(val)} {suffix}"
            
            with st.container(border=True):
                c1, c2 = st.columns([1, 5])
                with c1: st.markdown(f"<h1 style='text-align: center;'>{icon}</h1>", unsafe_allow_html=True)
                with c2:
                    st.markdown(f"**{title}**")
                    st.markdown(f"### 👑 {winner} — {val_str}")
                    with st.expander("Показать весь топ"):
                        display_df = res.to_frame(name="Значение")
                        if is_duration:
                            display_df['Значение'] = display_df['Значение'].apply(format_duration)
                        else:
                            display_df['Значение'] = display_df['Значение'].astype(int).astype(str) + f" {suffix}"
                        st.dataframe(display_df, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            leaderboard("Король Хайпа (Лайки)", "❤️", "reaction_count", suffix="лайков", use_full_df=True)
            leaderboard("Золотой Микрофон (ГС)", "🎙️", "voice_duration", is_duration=True)
            leaderboard("Токсичный Мститель", "🤬", "bad_count", suffix="матов")
        
        with c2:
            leaderboard("Спилберг (Кружочки)", "📹", "video_duration", is_duration=True)
            leaderboard("Лев Толстой (Текст)", "📜", "text_len", agg_func='mean', suffix="симв. (среднее)")
            leaderboard("Главный Хохотун", "😂", "has_laugh", suffix="кеков")

    # --- 3. РЕЙТИНГ ---
    with tab_psycho:
        st.header("🧠 Рейтинг Личностей")
        st.info("""
        **Как это считается:**
        * **Токсичность:** Количество матерных корней на 1000 сообщений.
        * **Веселье:** Количество сообщений со смехом (ахах, лол, кек) на 1000 сообщений.
        * **Респект:** Абсолютное количество полученных реакций (лайков).
        * **Душнила:** Средняя длина сообщения в символах.
        """)
        
        stats = df_clean.groupby('username').agg({
            'bad_count': 'sum',
            'has_laugh': 'sum',
            'reaction_count': 'sum',
            'text_len': 'mean',
            'message_id': 'count'
        }).reset_index()
        
        stats['Токсичность'] = stats['bad_count'] / stats['message_id'] * 1000
        stats['Веселье'] = stats['has_laugh'] / stats['message_id'] * 1000
        stats['Респект (Лайки)'] = stats['reaction_count']
        stats['Душнила (Длина)'] = stats['text_len']
        
        final_table = stats[['username', 'Токсичность', 'Веселье', 'Респект (Лайки)', 'Душнила (Длина)']].set_index('username')
        
        max_toxic = float(final_table['Токсичность'].max())
        max_fun = float(final_table['Веселье'].max())
        max_respect = int(final_table['Респект (Лайки)'].max())
        max_smart = int(final_table['Душнила (Длина)'].max())

        st.dataframe(
            final_table,
            column_config={
                "Токсичность": st.column_config.ProgressColumn("Токсичность 🤬", format="%.1f", min_value=0, max_value=max_toxic),
                "Веселье": st.column_config.ProgressColumn("Веселье 😂", format="%.1f", min_value=0, max_value=max_fun),
                "Респект (Лайки)": st.column_config.ProgressColumn("Респект ❤️", format="%d", min_value=0, max_value=max_respect),
                "Душнила (Длина)": st.column_config.ProgressColumn("Душнила 🤓", format="%d", min_value=0, max_value=max_smart),
            },
            use_container_width=True,
            height=600
        )

    # --- 4. СВЯЗИ (UNIVERSAL) ---
    with tab_mentions:
        st.header("🔗 Социальные связи")
        if not df_mentions.empty:
            # 1. Берем маппинг из конфига
            # Приводим ключи к нижнему регистру на всякий случай
            clean_map = {k.lower(): v for k, v in TAG_MAPPING.items()}
            
            # 2. Очистка и фильтрация
            df_mentions['target_lower'] = df_mentions['target_name'].str.lower().str.strip()
            
            # Оставляем только тех, кто есть в конфиге
            df_mentions_filtered = df_mentions[df_mentions['target_lower'].isin(clean_map.keys())].copy()
            df_mentions_filtered['target_display'] = df_mentions_filtered['target_lower'].map(clean_map)
            
            if not df_mentions_filtered.empty:
                c1, c2 = st.columns(2)
                
                # Топ кого тегают
                top_targets = df_mentions_filtered['target_display'].value_counts().head(10).reset_index()
                top_targets.columns = ['Кого тегают', 'Раз']
                c1.subheader("Самые популярные")
                c1.dataframe(top_targets, use_container_width=True)
                
                # Топ кто тегает
                top_sources = df_mentions_filtered['source_username'].value_counts().head(10).reset_index()
                top_sources.columns = ['Кто зовет', 'Раз']
                c2.subheader("Самые общительные")
                c2.dataframe(top_sources, use_container_width=True)
                
                st.divider()
                
                # --- МАТРИЦА ---
                st.subheader("Матрица упоминаний")
                st.caption("Кто (Y) кого (X) тегал. Сортировка по активности.")
                
                matrix = df_mentions_filtered.groupby(['source_username', 'target_display']).size().reset_index(name='count')
                
                # Сортировка осей
                top_src = matrix.groupby('source_username')['count'].sum().sort_values(ascending=True).index.tolist()
                top_tgt = matrix.groupby('target_display')['count'].sum().sort_values(ascending=True).index.tolist()
                
                fig_matrix = px.density_heatmap(
                    matrix, 
                    x='target_display', 
                    y='source_username', 
                    z='count', 
                    color_continuous_scale='Viridis',
                    template="plotly_dark",
                    labels={'target_display': 'Кого тегали', 'source_username': 'Кто тегал'},
                    category_orders={
                        "source_username": top_src,
                        "target_display": top_tgt
                    }
                )
                fig_matrix.update_layout(height=600)
                st.plotly_chart(fig_matrix, use_container_width=True)
            else:
                st.warning("Упоминания есть, но они не совпадают с TAG_MAPPING в конфиге.")
        else:
            st.warning("База упоминаний пуста.")
            
    # --- 5. ЛЕКСИКА ---
    with tab_words:
        st.header("🗣️ Любимые словечки")
        st.markdown("Топ-10 самых частых слов (без репостов, тегов и мусора).")
        users = df['username'].unique()
        cols = st.columns(3)
        for i, user in enumerate(users):
            with cols[i % 3]:
                with st.container(border=True):
                    st.subheader(user)
                    top = get_top_words(df, user)
                    if top:
                        for word, count in top:
                            st.markdown(f"**{word}** — {count}")
                    else:
                        st.caption("Мало данных")

    # --- 6. ИГРА ---
    with tab_game:
        st.header("🎮 Битва Интуиций")
        def generate_quiz(df):
            questions = []
            authors = list(df['username'].unique())
            if len(authors) < 2: return []
            
            questions.append({"q": "Кто написал больше всех сообщений?", "opts": authors, "a": df_clean['username'].value_counts().idxmax()})
            questions.append({"q": "Кто собрал больше всех лайков?", "opts": authors, "a": df.groupby('username')['reaction_count'].sum().idxmax()})
            
            voice_w = df.groupby('username')['voice_duration'].sum().idxmax()
            questions.append({"q": "Кто наговорил больше всего времени в ГС?", "opts": authors, "a": voice_w})

            quotes = df_clean[(df_clean['word_count'] > 5) & (df_clean['word_count'] < 15) & (df_clean['media_type'] == 'text')].sample(10)
            for _, row in quotes.iterrows():
                questions.append({"q": f"Чья цитата: «{row['text']}»?", "opts": authors, "a": row['username']})

            random.shuffle(questions)
            return questions[:15]

        if 'quiz_data' not in st.session_state or st.button("🎲 Начать заново"):
            st.session_state.quiz_data = generate_quiz(df)
            st.session_state.q_idx = 0
            st.session_state.score = 0
            st.session_state.game_over = False

        if not st.session_state.game_over and st.session_state.quiz_data:
            q = st.session_state.quiz_data[st.session_state.q_idx]
            st.progress((st.session_state.q_idx) / len(st.session_state.quiz_data))
            st.markdown(f"### {q['q']}")
            cols = st.columns(3)
            opts = q['opts']
            random.shuffle(opts)
            for i, opt in enumerate(opts):
                if cols[i % 3].button(opt, use_container_width=True):
                    if opt == q['a']:
                        st.toast("✅ Верно!", icon="🎉")
                        st.session_state.score += 1
                    else:
                        st.toast(f"❌ Ошибка! Это был {q['a']}", icon="💩")
                    if st.session_state.q_idx < len(st.session_state.quiz_data) - 1:
                        st.session_state.q_idx += 1
                        st.rerun()
                    else:
                        st.session_state.game_over = True
                        st.rerun()
        elif st.session_state.game_over:
            st.balloons()
            st.success(f"Финиш! Твой счет: {st.session_state.score} из {len(st.session_state.quiz_data)}")

    # --- 7. ПОИСК ---
    with tab_search:
        st.header("🕵️ Поиск")
        query = st.text_input("Поиск...", placeholder="Введите фразу")
        if query:
            mask = df['text'].str.contains(query, case=False, na=False)
            res = df[mask].sort_values('date', ascending=False)
            st.info(f"Найдено: {len(res)}")
            for _, row in res.head(10).iterrows():
                with st.chat_message(row['username']):
                    st.write(f"**{row['username']}** ({row['date'].strftime('%d.%m %Y')})")
                    st.write(row['text'])
                    if row['reaction_count'] > 0:
                        st.caption(f"❤️ {row['reaction_count']}")
                    if row['is_forwarded']:
                        st.caption("↪️ Пересланное сообщение")

if __name__ == "__main__":
    main()