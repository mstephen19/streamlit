import { useCallback, useState } from 'react';
import reactLogo from './assets/react.svg';
import viteLogo from '/vite.svg';
import { pubSubHooks } from 'streamlit-react';
import './App.css';

const { usePublish, useSubscribe } = pubSubHooks<{
    messages: 'testmessage' | 'testmessage2';
}>({
    baseUrl: 'https://localhost:8000',
    path: '/events',
});

function App() {
    const publish = usePublish({
        namespace: 'messages',
        key: 'test',
    });

    useSubscribe({
        namespace: 'messages',
        key: 'test',
        eventName: 'testmessage',
        eventHandler: useCallback((data) => {
            console.log(data);
        }, []),
    });

    useSubscribe({
        namespace: 'messages',
        key: 'test',
        eventName: 'testmessage2',
        eventHandler: useCallback((data) => {
            console.log(data);
        }, []),
    });

    return (
        <>
            <div>
                <a href='https://vite.dev' target='_blank'>
                    <img src={viteLogo} className='logo' alt='Vite logo' />
                </a>
                <a href='https://react.dev' target='_blank'>
                    <img src={reactLogo} className='logo react' alt='React logo' />
                </a>
            </div>
            <h1>Vite + React</h1>
            <div className='card'>
                <button
                    onClick={() =>
                        publish(
                            'testmessage',
                            JSON.stringify({
                                content: 'hello',
                            })
                        )
                    }>
                    testmessage
                </button>

                <button
                    onClick={() =>
                        publish(
                            'testmessage2',
                            JSON.stringify({
                                content: 'hello 2',
                            })
                        )
                    }>
                    testmessage2
                </button>
                <p>
                    Edit <code>src/App.tsx</code> and save to test HMR
                </p>
            </div>
            <p className='read-the-docs'>Click on the Vite and React logos to learn more</p>
        </>
    );
}

export default App;
