namespace Sooda
{
    using System.Runtime.CompilerServices;

    internal static class InitSooda
    {
        [MethodImpl(MethodImplOptions.ForwardRef)]
        internal static void Check()
        {
            //// Токена отклытого ключа, нашей кючевой пары 
            //byte[] my_public_key = { 0x6a, 0x8a, 0xb3, 0x7d, 0xb6, 0x2c, 0x1c, 0x9c };

            //// Значение токена отклытого ключа, 
            //// с использованием которой подписана сборка 
            //byte[] public_key = Assembly.GetExecutingAssembly().GetName().GetPublicKeyToken();

            //// Если сборка не подписана, то это попытка модификации кода 
            //if (public_key == null
            //|| public_key.Length != my_public_key.Length) throw new ApplicationException();

            //for (int i = 0; i < my_public_key.Length; i++)
            //{
            //    // Если токены не совпадают, то это попытка модификации кода 
            //    if (public_key[i] != my_public_key[i]) throw new ApplicationException();
            //}
        }
    }
}